const executablePath = process.env.PW_CHROMIUM_EXECUTABLE_PATH

/** @type {import('playwright/test').PlaywrightTestConfig} */
module.exports = {
  projects: [
    {
      name: 'browser-perf',
      use: {
        browserName: 'chromium',
        ...(executablePath
          ? {
              launchOptions: {
                executablePath,
              },
            }
          : {}),
      },
    },
  ],
}
