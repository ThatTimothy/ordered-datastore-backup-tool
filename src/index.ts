import getConfig from "./config"
import run from "./download"

async function main() {
    const config = await getConfig()
    run(config)
}

main()
