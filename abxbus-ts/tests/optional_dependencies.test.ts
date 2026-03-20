import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const ts_root = join(tests_dir, '..')
const package_json_path = join(ts_root, 'package.json')
const src_dir = join(ts_root, 'src')

type PackageJSON = {
  dependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
  devDependencies?: Record<string, string>
  peerDependencies?: Record<string, string>
}

const loadPackageJson = (): PackageJSON => JSON.parse(readFileSync(package_json_path, 'utf8')) as PackageJSON

test('bridge dependencies are optional in package.json', () => {
  const package_json = loadPackageJson()
  const dependencies = package_json.dependencies ?? {}
  const optional_dependencies = package_json.optionalDependencies ?? {}

  assert.equal(Object.hasOwn(dependencies, 'ioredis'), false)
  assert.equal(Object.hasOwn(dependencies, 'nats'), false)
  assert.equal(Object.hasOwn(dependencies, 'pg'), false)

  assert.equal(Object.hasOwn(optional_dependencies, 'ioredis'), true)
  assert.equal(Object.hasOwn(optional_dependencies, 'nats'), true)
  assert.equal(Object.hasOwn(optional_dependencies, 'pg'), true)
})

test('package.json does not depend on third-party sqlite packages', () => {
  const package_json = loadPackageJson()
  const sqlite_package_names = ['sqlite3', 'better-sqlite3', '@sqlite.org/sqlite-wasm']
  const dependency_sections = [
    package_json.dependencies ?? {},
    package_json.optionalDependencies ?? {},
    package_json.devDependencies ?? {},
    package_json.peerDependencies ?? {},
  ]

  for (const section of dependency_sections) {
    for (const sqlite_package_name of sqlite_package_names) {
      assert.equal(Object.hasOwn(section, sqlite_package_name), false, `unexpected sqlite package: ${sqlite_package_name}`)
    }
  }
})

test('bridge modules do not statically import optional bridge packages', () => {
  const bridge_modules = [
    {
      path: join(src_dir, 'bridge_redis.ts'),
      forbidden_patterns: [/from\s+['"]ioredis['"]/, /import\s+['"]ioredis['"]/],
      required_pattern: /importOptionalDependency\('RedisEventBridge', 'ioredis'\)/,
    },
    {
      path: join(src_dir, 'bridge_nats.ts'),
      forbidden_patterns: [/from\s+['"]nats['"]/, /import\s+['"]nats['"]/],
      required_pattern: /importOptionalDependency\('NATSEventBridge', 'nats'\)/,
    },
    {
      path: join(src_dir, 'bridge_postgres.ts'),
      forbidden_patterns: [/from\s+['"]pg['"]/, /import\s+['"]pg['"]/],
      required_pattern: /importOptionalDependency\('PostgresEventBridge', 'pg'\)/,
    },
  ]

  for (const bridge_module of bridge_modules) {
    const source = readFileSync(bridge_module.path, 'utf8')
    for (const forbidden_pattern of bridge_module.forbidden_patterns) {
      assert.equal(forbidden_pattern.test(source), false, `${bridge_module.path} has eager optional dependency import`)
    }
    assert.equal(bridge_module.required_pattern.test(source), true, `${bridge_module.path} must use lazy optional dependency import`)
  }
})
