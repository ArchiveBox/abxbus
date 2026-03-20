const fs = require('fs')
const http = require('http')
const path = require('path')
const { test, expect } = require('playwright/test')

test.describe('browser runtime perf', () => {
  test.setTimeout(120_000)

  test('runs shared perf scenarios in Chromium JS runtime', async ({ page, browserName }) => {
    expect(browserName).toBe('chromium')

    const rootDir = path.resolve(__dirname, '..')
    const server = http.createServer((req, res) => {
      const requestUrl = new URL(req.url || '/', 'http://127.0.0.1')
      const pathname = decodeURIComponent(requestUrl.pathname)

      if (pathname === '/' || pathname === '/index.html') {
        res.statusCode = 200
        res.setHeader('content-type', 'text/html; charset=utf-8')
        res.end(`<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <script type="importmap">
      {
        "imports": {
          "uuid": "/node_modules/uuid/dist/esm-browser/index.js",
          "zod": "/node_modules/zod/index.js"
        }
      }
    </script>
  </head>
  <body>browser perf harness</body>
</html>`)
        return
      }

      const absolutePath = path.resolve(rootDir, `.${pathname}`)
      const relativePath = path.relative(rootDir, absolutePath)
      if (relativePath.startsWith('..') || path.isAbsolute(relativePath)) {
        res.statusCode = 403
        res.end('forbidden')
        return
      }
      if (!fs.existsSync(absolutePath) || fs.statSync(absolutePath).isDirectory()) {
        res.statusCode = 404
        res.end('not found')
        return
      }

      const ext = path.extname(absolutePath)
      if (ext === '.js' || ext === '.mjs' || ext === '.cjs') {
        res.setHeader('content-type', 'text/javascript; charset=utf-8')
      } else if (ext === '.map' || ext === '.json') {
        res.setHeader('content-type', 'application/json; charset=utf-8')
      } else if (ext === '.html') {
        res.setHeader('content-type', 'text/html; charset=utf-8')
      } else {
        res.setHeader('content-type', 'text/plain; charset=utf-8')
      }

      res.statusCode = 200
      res.end(fs.readFileSync(absolutePath))
    })

    await new Promise((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', () => {
        server.removeListener('error', reject)
        resolve()
      })
    })

    const address = server.address()
    if (!address || typeof address === 'string') {
      await new Promise((resolve) => server.close(() => resolve()))
      throw new Error('failed to resolve browser perf server address')
    }
    const baseUrl = `http://127.0.0.1:${address.port}`

    let result
    try {
      await page.goto(`${baseUrl}/`)
      result = await page.evaluate(async () => {
        const [api, scenarios] = await Promise.all([import('/dist/esm/index.js'), import('/tests/performance.scenarios.js')])
        const logs = []

        const results = await scenarios.runAllPerfScenarios({
          runtimeName: 'chromium-js',
          api: {
            BaseEvent: api.BaseEvent,
            EventBus: api.EventBus,
            EventHandlerTimeoutError: api.EventHandlerTimeoutError,
            EventHandlerCancelledError: api.EventHandlerCancelledError,
          },
          now: () => performance.now(),
          sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
          log: (message) => logs.push(message),
          limits: {
            singleRunMs: 30_000,
            worstCaseMs: 60_000,
          },
        })

        return { logs, results }
      })
    } finally {
      await new Promise((resolve) => server.close(() => resolve()))
    }

    for (const line of result.logs) {
      console.log(line)
    }

    expect(result.results.length).toBe(6)
  })
})
