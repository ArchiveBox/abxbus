export const isNodeRuntime = (): boolean => {
  const maybe_process = (globalThis as { process?: { versions?: { node?: string } } }).process
  return typeof maybe_process?.versions?.node === 'string'
}

const missingDependencyError = (bridge_name: string, package_name: string): Error =>
  new Error(`${bridge_name} requires optional dependency "${package_name}". Install it with: npm install ${package_name}`)

export const assertOptionalDependencyAvailable = (bridge_name: string, package_name: string): void => {
  if (!isNodeRuntime()) return

  const maybe_process = (
    globalThis as {
      process?: { getBuiltinModule?: (name: string) => any; cwd?: () => string }
    }
  ).process
  const get_builtin_module = maybe_process?.getBuiltinModule

  let require_fn: { resolve: (specifier: string) => string } | undefined
  try {
    require_fn = Function('return typeof require === "function" ? require : undefined')() as
      | { resolve: (specifier: string) => string }
      | undefined
  } catch {
    require_fn = undefined
  }

  if (!require_fn && typeof get_builtin_module === 'function') {
    const module_builtin = get_builtin_module('module')
    const create_require = module_builtin?.createRequire
    if (typeof create_require === 'function') {
      const cwd = typeof maybe_process?.cwd === 'function' ? maybe_process.cwd() : '.'
      require_fn = create_require(`${cwd}/package.json`) as { resolve: (specifier: string) => string }
    }
  }

  if (!require_fn) return
  try {
    require_fn.resolve(package_name)
  } catch {
    throw missingDependencyError(bridge_name, package_name)
  }
}

export const importOptionalDependency = async (bridge_name: string, package_name: string): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  try {
    return (await dynamic_import(package_name)) as any
  } catch {
    throw missingDependencyError(bridge_name, package_name)
  }
}
