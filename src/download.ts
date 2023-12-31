import fetch, { RequestInit } from "node-fetch"
import async from "async"
import { WriteStream, createWriteStream } from "fs"
import { open } from "fs/promises"
import path from "path"
import { formatSeconds, pathExists } from "./util"
import { Config } from "./config"

const DATA_OUTPUT = "data.csv"
const LOG_STORE_PATH = "log.txt"

const ORDERED_DATASTORE_BASE =
    "https://apis.roblox.com/ordered-data-stores/v1/universes/"
const USER_AGENT =
    "ordered-datastore-backup-tool (https://github.com/ThisStudio/ordered-datastore-backup-tool)"
// If we get ratelimited / server error, we want to backoff for an amount of time
const STARTING_BACKOFF = 15 // This is the backoff we start at
const INCREMENT_BACKOFF = 10 // Every repeated backoff we have increments the amount we wait by this
const INCREMENT_RETURN_BACKOFF = 0.1 // Every successful request brings us back to STARTING_BACKOFF by this increment

interface Position {
    nextPageToken: string | null
    page: number
}

interface WriteTask {
    json: unknown
    logMessage: string
}

function sleep(seconds: number) {
    return new Promise((resolve) => setTimeout(resolve, seconds * 1000))
}

/* eslint-disable  @typescript-eslint/no-explicit-any */ // Because chunk is already any
function write(stream: WriteStream, chunk: any): Promise<void> {
    return new Promise((resolve, reject) => {
        const drained = stream.write(chunk, reject)

        if (drained) {
            resolve()
        } else {
            stream.once("drain", resolve)
        }
    })
}

// Gets the last position from the log
async function getLastPosition(config: Config): Promise<Position> {
    const logPath = path.join(config.OutputDir, LOG_STORE_PATH)
    const exists = pathExists(logPath)

    const result: Position = {
        nextPageToken: null,
        page: 1,
    }

    if (!exists) {
        return result
    }

    const file = await open(logPath)
    let i = 2

    // Read line by line, updating the latest working pageToken we find
    for await (const line of file.readLines()) {
        if (line.length == 0) {
            continue
        }

        result.page = i

        const firstSpace = line.indexOf(" ")
        let nextPageToken = line.substring(0, firstSpace)
        if (nextPageToken == "null") {
            nextPageToken = null
        }
        result.nextPageToken = nextPageToken

        i += 1
    }

    return result
}

async function downloadPage(
    position: Position,
    BASE_URL: string,
    options: RequestInit,
): Promise<unknown> {
    // Initialize request
    const url = new URL(BASE_URL)
    url.searchParams.append("max_page_size", "100")
    url.searchParams.append("order_by", "desc")
    if (position.nextPageToken) {
        url.searchParams.append("page_token", position.nextPageToken)
    }

    // Make request
    const response = await fetch(url, options).catch((reason) => {
        throw new Error(`Failed to fetch ${url}: ${reason}`)
    })

    // Validate text
    const text = await response.text().catch(async (reason) => {
        throw new Error(
            `Couldn't get response text at ${url}, ${response.status} ${response.statusText}, error: ${reason}`,
        )
    })

    // Validate 200-level status, excluding 429 to be handled later
    if (
        response.status != 429 &&
        response.status < 200 &&
        response.status >= 300
    ) {
        throw new Error(
            `Unexpected error at ${url}, ${response.status} ${response.statusText}: ${text}`,
        )
    }

    // Handle 429 == hit ratelimit
    if (response.status == 429) {
        throw new Error(
            `Ratelimit at ${url}, ${response.status} ${response.statusText}`,
        )
    }

    // Parse to json
    const json = await new Promise((resolve) => {
        try {
            const json = JSON.parse(text)
            resolve(json)
        } catch (reason) {
            throw new Error(
                `Couldn't parse to JSON at ${url}, ${response.status} ${response.statusText}: ${reason}, raw text: ${text}`,
            )
        }
    })

    // Okay, we should be good now
    const entries = json["entries"]

    // But we validate the data structure because it's Roblox
    if (!entries) {
        throw new Error(
            `Got invalid data structure at ${url}, ${response.status} ${
                response.statusText
            }: ${JSON.stringify(json, null, 4)}`,
        )
    }

    // Actually good now
    return json
}

export default async function run(config: Config) {
    // Try to load the last position if we can
    const position = await getLastPosition(config)

    // Output resume message if resuming
    if (position.page != 1) {
        console.log(`Resuming previous download at page ${position.page}`)
    }

    // Initialize everything
    let backoff = STARTING_BACKOFF
    let running = true

    const BASE_URL = `${ORDERED_DATASTORE_BASE}${config.Id}/orderedDataStores/${
        config.Name
    }/scopes/${config.Scope || "global"}/entries`

    const options: RequestInit = {
        headers: {
            "User-Agent": USER_AGENT,
            Accept: "*/*",
            "x-api-key": config.Key,
        },
    }

    const dataStream = createWriteStream(
        path.join(config.OutputDir, DATA_OUTPUT),
        {
            flags: "a",
        },
    )

    const logStream = createWriteStream(
        path.join(config.OutputDir, LOG_STORE_PATH),
        {
            flags: "a",
        },
    )

    const writeTaskQueue = async.queue<WriteTask>(async (task, callback) => {
        if (typeof task.json != "object") {
            return
        }

        const entries = task.json["entries"]

        if (!entries) {
            return
        }

        for (const entry of Object.values(entries)) {
            const id = entry["id"]
            const value = entry["value"]

            if (id && typeof(value) === "number") {
                await write(dataStream, `${id},${value}\n`)
            }
        }

        await write(logStream, task.logMessage + "\n")

        callback()
    }, 1)

    writeTaskQueue.error((error, task) => {
        console.error(`Error: ${error} on task: ${task}`)
    })

    // Define teardown function for when we are done OR if we need to exit early
    const teardown = async () => {
        running = false

        // Wait for write task queue to deplete
        if (!writeTaskQueue.idle()) {
            await writeTaskQueue.drain()
        }

        // Drain fires when 1 payload is left sometimes, so need to verify
        while (writeTaskQueue.payload > 0) {
            await sleep(0.1)
        }

        console.log("Finished writing")

        // Close the open streams
        await new Promise<void>((resolve) => {
            if (dataStream.closed && logStream.closed) {
                resolve()
            }

            dataStream.end(() => {
                logStream.end(() => {
                    resolve()
                })
            })
        })

        console.log("Closed write streams")
    }

    // Bind teardown to process exit
    ;["SIGINT", "SIGTERM", "SIGQUIT"].forEach((signal) => {
        process.on(signal, teardown)
    })

    console.log("Set up write streams, starting download...")

    // Begin main download loop
    while (running) {
        // Download this page, if failed output error
        const maybeJson = await downloadPage(position, BASE_URL, options).catch(
            (reason) => console.error(reason),
        )

        // If something went wrong, backoff
        if (!maybeJson || !maybeJson["entries"]) {
            console.log(`Backing off for ${formatSeconds(backoff)}s`)
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        // Otherwise, add the write task to the task queue
        const json = maybeJson
        const nextToken = json["nextPageToken"]

        writeTaskQueue.push({
            json: json,
            logMessage: `${nextToken} <- Next page token | Got page ${position.page} (${position.nextPageToken})`,
        })

        // Output status update
        if (position.page == 1 || position.page % 10 == 0) {
            console.log(`Got page ${position.page}`)
        }

        // If no next token, we are done!
        if (!nextToken) {
            console.log("No next page, finished download!")
            break
        }

        // Update variables for next loop
        backoff = Math.max(backoff - INCREMENT_RETURN_BACKOFF, STARTING_BACKOFF) // Slowly return to normal after success
        position.nextPageToken = nextToken
        position.page += 1
    }

    // Finally, teardown
    await teardown()

    console.log("Done")
}
