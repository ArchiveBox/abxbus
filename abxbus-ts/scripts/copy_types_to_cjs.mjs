import { cp } from 'node:fs/promises'

await cp('dist/types', 'dist/cjs', { recursive: true, force: true })
