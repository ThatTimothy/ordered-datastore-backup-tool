import fetch, { RequestInit } from "node-fetch"
import async from "async"
import { WriteStream, createWriteStream } from "fs"
import { open } from "fs/promises"
import path from "path"
import { pathExists } from "./util"
import { Config } from "./config"

const DATA_OUTPUT = "data.csv"
const LOG_STORE_PATH = "log.txt"

const ORDERED_DATASTORE_BASE =
    "https://apis.roblox.com/ordered-data-stores/v1/universes/"
const USER_AGENT =
    "ordered-datastore-backup-tool (https://github.com/ThisStudio/ordered-datastore-backup-tool)"
// If we get ratelimited / server error, we want to backoff for an amount of time
const STARTING_BACKOFF = 5 // This is the backoff we start at
const INCREMENT_BACKOFF = 5 // Every repeated backoff we have increments the amount we wait by this

interface Position {
    nextPageToken: string | null
    page: number
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

export default async function run(config: Config) {
    const position = await getLastPosition(config)

    if (position.page != 1) {
        console.log(`Resuming previous download at page ${position.page}`)
    }

    let backoff = STARTING_BACKOFF

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

    ;["SIGINT", "SIGTERM", "SIGQUIT"].forEach((signal) => {
        process.on(signal, () => {
            if (dataStream.closed) {
                return
            }
            dataStream.end(() => {
                logStream.end(() => {
                    console.log("Closed write streams")
                    process.exit()
                })
            })
        })
    })

    const taskQueue = async.queue<unknown>(async (json, callback) => {
        if (typeof json != "object") {
            return
        }

        const entries = json["entries"]

        if (!entries) {
            return
        }

        for (const entry of Object.values(entries)) {
            const id = entry["id"]
            const value = entry["value"]

            if (id && value) {
                await write(dataStream, `${id},${value}\n`)
            }
        }

        callback()
    }, 1)

    taskQueue.error((error, task) => {
        console.error(`Error: ${error} on task: ${task}`)
    })

    const logQueue = async.queue<string>(async (message, callback) => {
        await write(logStream, message + "\n")

        callback()
    }, 1)

    logQueue.error((error, log) => {
        console.error(`Error: ${error} on log: ${log}`)
    })

    console.log("Set up write streams, starting download...")

    while (true) {
        const url = new URL(BASE_URL)
        url.searchParams.append("max_page_size", "100")
        url.searchParams.append("order_by", "desc")
        if (position.nextPageToken) {
            url.searchParams.append("page_token", position.nextPageToken)
        }

        const maybeResponse = await fetch(url, options).catch((reason) =>
            console.error(reason),
        )

        // Response didn't occur, something went wrong
        if (!maybeResponse) {
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        const response = maybeResponse
        const maybeText = await response
            .text()
            .catch(async (reason) =>
                console.error(
                    `Couldn't get response text, code ${response.status} ${response.statusText}, error: ${reason}`,
                ),
            )

        if (!maybeText) {
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        const text = maybeText

        const maybeJson = await new Promise((resolve) => {
            try {
                const json = JSON.parse(text)
                resolve(json)
            } catch (e) {
                console.error(`Couldn't parse to JSON: ${e}, raw text: ${text}`)
            }
        })

        if (!maybeJson) {
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        const json = maybeJson

        if (
            response.status != 429 &&
            response.status < 200 &&
            response.status >= 300
        ) {
            console.error(`Unexpected error ${response.status}: ${json}`)
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        if (response.status == 429) {
            console.error(`Ratelimit, backing off`)
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        // Okay, we should be good now
        const entries = json["entries"]
        const nextToken = json["nextPageToken"]

        // But we validate the data structure because it's Roblox
        if (!entries) {
            console.error(
                `Got invalid data structure at ${response.url} (${
                    response.status
                } ${response.statusText}): ${JSON.stringify(json, null, 4)}`,
            )
            await sleep(backoff)
            backoff += INCREMENT_BACKOFF
            continue
        }

        if (position.page == 1 || position.page % 10 == 0) {
            console.log(`Got page ${position.page}`)
        }

        taskQueue.push(json)
        logQueue.push(
            `${nextToken} <- Next page token | Got page ${position.page} (${position.nextPageToken})`,
        )

        // If no next token, we are done!
        if (!nextToken) {
            console.log("No next page, finished!")
            break
        }

        backoff = STARTING_BACKOFF
        position.nextPageToken = nextToken
        position.page += 1
    }

    if (!taskQueue.idle()) {
        await taskQueue.drain()
    }

    if (!logQueue.idle()) {
        await logQueue.drain()
    }

    console.log("Finished writing")

    await new Promise<void>((resolve) => {
        dataStream.end(() => {
            logStream.end(() => {
                resolve()
            })
        })
    })

    console.log("Closed write streams")
}
