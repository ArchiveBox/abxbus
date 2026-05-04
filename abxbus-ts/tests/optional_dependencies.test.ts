import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
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
  peerDependenciesMeta?: Record<string, { optional?: boolean }>
  exports?: Record<string, string | Record<string, string>>
}

const loadPackageJson = (): PackageJSON => JSON.parse(readFileSync(package_json_path, 'utf8')) as PackageJSON

test('bridge and optional middleware dependencies are optional peers in package.json', () => {
  const package_json = loadPackageJson()
  const dependencies = package_json.dependencies ?? {}
  const peer_dependencies = package_json.peerDependencies ?? {}
  const peer_dependencies_meta = package_json.peerDependenciesMeta ?? {}

  for (const package_name of [
    'ioredis',
    'nats',
    'pg',
    '@tachyon-ipc/core',
    '@opentelemetry/api',
    '@opentelemetry/exporter-trace-otlp-http',
    '@opentelemetry/resources',
    '@opentelemetry/sdk-trace-base',
  ]) {
    assert.equal(Object.hasOwn(dependencies, package_name), false)
    assert.equal(Object.hasOwn(peer_dependencies, package_name), true)
    assert.equal(peer_dependencies_meta[package_name]?.optional, true)
  }
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

test('package.json exposes source subpaths for direct TypeScript source imports', () => {
  const package_json = loadPackageJson()
  const exports_map = package_json.exports ?? {}

  assert.deepEqual(exports_map['./source'], {
    source: './src/index.ts',
    types: './src/index.ts',
    import: './src/index.ts',
    default: './src/index.ts',
  })
  assert.deepEqual(exports_map['./source/*'], {
    source: './src/*.ts',
    types: './src/*.ts',
    import: './src/*.ts',
    default: './src/*.ts',
  })
})

test('bridge and middleware implementation filenames match exported class names', () => {
  const source_files = new Set(readdirSync(src_dir))

  for (const filename of [
    'EventBridge.ts',
    'HTTPEventBridge.ts',
    'SocketEventBridge.ts',
    'JSONLEventBridge.ts',
    'SQLiteEventBridge.ts',
    'RedisEventBridge.ts',
    'NATSEventBridge.ts',
    'PostgresEventBridge.ts',
    'TachyonEventBridge.ts',
    'EventBusMiddleware.ts',
    'OtelTracingMiddleware.ts',
  ]) {
    assert.equal(source_files.has(filename), true, `missing class-named implementation file: ${filename}`)
  }

  for (const filename of source_files) {
    assert.equal(/^bridge_|^middleware_/.test(filename), false, `implementation file should use class name: ${filename}`)
  }
})

test('bridge modules do not statically import optional bridge packages', () => {
  const bridge_modules = [
    {
      path: join(src_dir, 'RedisEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]ioredis['"]/, /import\s+['"]ioredis['"]/],
      required_pattern: /importOptionalDependency\('RedisEventBridge', 'ioredis'\)/,
    },
    {
      path: join(src_dir, 'NATSEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]nats['"]/, /import\s+['"]nats['"]/],
      required_pattern: /importOptionalDependency\('NATSEventBridge', 'nats'\)/,
    },
    {
      path: join(src_dir, 'PostgresEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]pg['"]/, /import\s+['"]pg['"]/],
      required_pattern: /importOptionalDependency\('PostgresEventBridge', 'pg'\)/,
    },
    {
      path: join(src_dir, 'TachyonEventBridge.ts'),
      forbidden_patterns: [/from\s+['"]@tachyon-ipc\/core['"]/, /import\s+['"]@tachyon-ipc\/core['"]/],
      required_pattern: /assertOptionalDependencyAvailable\('TachyonEventBridge', '@tachyon-ipc\/core'\)/,
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

test('root import excludes peer-backed integrations while namespaced imports resolve them', async () => {
  const root_source = readFileSync(join(src_dir, 'index.ts'), 'utf8')
  for (const forbidden_import of [
    'PostgresEventBridge',
    'RedisEventBridge',
    'NATSEventBridge',
    'TachyonEventBridge',
    'OtelTracingMiddleware',
    '@opentelemetry',
    'ioredis',
    'nats',
    'pg',
    '@tachyon-ipc/core',
  ]) {
    assert.equal(root_source.includes(forbidden_import), false, `root index pulls in ${forbidden_import}`)
  }
  assert.equal(root_source.includes('EventBusMiddleware'), true, 'root index should type-export base middleware types')

  const root = await import('../src/index.js')
  for (const root_export of ['EventBus', 'EventBridge', 'HTTPEventBridge', 'JSONLEventBridge', 'SQLiteEventBridge']) {
    assert.equal(root_export in root, true, `missing root export ${root_export}`)
  }
  for (const peer_export of ['PostgresEventBridge', 'RedisEventBridge', 'NATSEventBridge', 'TachyonEventBridge', 'OtelTracingMiddleware']) {
    assert.equal(peer_export in root, false, `peer-backed export leaked into root: ${peer_export}`)
  }

  const bridges = await import('../src/bridges.js')
  const otel = await import('../src/OtelTracingMiddleware.js')
  assert.equal(typeof bridges.PostgresEventBridge, 'function')
  assert.equal(typeof bridges.TachyonEventBridge, 'function')
  assert.equal(typeof otel.OtelTracingMiddleware, 'function')
})
