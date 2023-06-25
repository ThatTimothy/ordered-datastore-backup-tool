import prompts from "prompts"
import { readFile, mkdir, writeFile } from "fs/promises"
import path from "path"
import { pathExists } from "./util"

const KEY_STORE_PATH = "key.txt"
const CONFIG_STORE_PATH = "config.json"
const DEFAULT_OUTPUT_PATH = "output"

const QUESTIONS: prompts.PromptObject[] = [
    {
        name: "OutputDir",
        type: "text",
        message: "Enter output directory:",
        initial: DEFAULT_OUTPUT_PATH,
    },
    {
        name: "Key",
        type: "password",
        message: "Enter API key:",
    },
    {
        name: "Id",
        type: "number",
        message: "Enter experience id:",
    },
    {
        name: "Name",
        type: "text",
        message: "Enter ordered datastore name:",
    },
    {
        name: "Scope",
        type: "text",
        message: "Enter ordered datastore scope (can be empty):",
    },
]

export interface Config {
    OutputDir: string
    Key: string
    Id: number
    Name: string
    Scope: string
}

type SavedConfig = Omit<Config, "Key">

async function saveConfig(saved: SavedConfig) {
    await writeFile(
        path.join(saved.OutputDir, CONFIG_STORE_PATH),
        JSON.stringify(saved, null, 4),
    )
}

export default async function getConfig(): Promise<Config> {
    const onSubmitHandler = (prompt, answer: string) => {
        // If the output dir entered already exists, we can pull config from there if it exists
        if (prompt.name == "OutputDir") {
            if (!answer || answer.length == 0) {
                return true
            }

            const exists = pathExists(answer)
            const keyExists =
                exists && pathExists(path.join(answer, KEY_STORE_PATH))
            const configExists =
                exists && pathExists(path.join(answer, CONFIG_STORE_PATH))

            return exists && keyExists && configExists
        }

        // Continue otherwise
        return false
    }

    const response = await prompts(QUESTIONS, {
        onSubmit: onSubmitHandler,
    })

    const outputDir: string | undefined = response["OutputDir"]
    if (!outputDir) {
        throw new Error("Must provide output directory!")
    }

    const outputKeyPath = path.join(outputDir, KEY_STORE_PATH)
    const outputKeyExists = pathExists(outputKeyPath)
    const outputConfigPath = path.join(outputDir, CONFIG_STORE_PATH)
    const outputConfigExists = pathExists(outputConfigPath)

    let key: string | undefined = response["Key"]

    if (outputKeyExists) {
        console.log(`Using key from ${outputKeyPath}`)
        key = await readFile(outputKeyPath).then((buffer) => buffer.toString())
    } else {
        await mkdir(outputDir, {
            recursive: true,
        })
    }

    if (!key) {
        throw new Error("Must provide valid key!")
    }

    if (!outputKeyExists) {
        await writeFile(outputKeyPath, key)
    }

    let existingConfig

    if (outputConfigExists) {
        existingConfig = await readFile(outputConfigPath).then((buffer) =>
            JSON.parse(buffer.toString()),
        )
    }

    const id: number | undefined = response["Id"] || existingConfig?.["Id"]

    if (!id || id <= 0) {
        throw new Error("Must provide valid, positive experience id!")
    }

    const name: string | undefined =
        response["Name"] || existingConfig?.["Name"]
    const scope: string =
        response["Scope"] || "global" || existingConfig?.["Scope"]

    if (!name) {
        throw new Error("Must provide valid ordered datastore name!")
    }

    await saveConfig({
        OutputDir: outputDir.trim(),
        Id: id,
        Name: name.trim(),
        Scope: scope.trim(),
    })

    return {
        OutputDir: outputDir.trim(),
        Key: key.trim(),
        Id: id,
        Name: name.trim(),
        Scope: scope.trim(),
    }
}
