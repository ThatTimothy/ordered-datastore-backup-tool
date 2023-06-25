import { accessSync } from "fs"

export function pathExists(path: string): boolean {
    try {
        accessSync(path)
        return true
    } catch {
        return false
    }
}
