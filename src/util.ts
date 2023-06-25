import { accessSync } from "fs"

export function pathExists(path: string): boolean {
    try {
        accessSync(path)
        return true
    } catch {
        return false
    }
}

export function formatSeconds(seconds: number): string {
    return (Math.round(seconds * 100) / 100).toFixed(2)
}
