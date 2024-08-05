export const sleep = async (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export const logInfo = (message: string, messengerId: string) => console.log(`[${messengerId}] ${message}`)

export const logError = (message: string, messengerId: string, error: unknown) => console.error(`[${messengerId}] ${message}`, error)
