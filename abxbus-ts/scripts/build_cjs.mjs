import { readdir, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

import { build } from 'esbuild'

const src_dir = 'src'
const dist_dir = 'dist/cjs'

const entries = (await readdir(src_dir))
  .filter((name) => name.endsWith('.ts') && !name.endsWith('.test.ts'))
  .map((name) => join(src_dir, name))

await rm(dist_dir, { recursive: true, force: true })

await build({
  entryPoints: entries,
  bundle: false,
  format: 'cjs',
  platform: 'neutral',
  target: 'es2022',
  sourcemap: true,
  outbase: src_dir,
  outdir: dist_dir,
})

await writeFile(join(dist_dir, 'package.json'), '{\n  "type": "commonjs",\n  "main": "./index.js",\n  "types": "./index.d.ts"\n}\n')
