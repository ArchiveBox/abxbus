import ts_parser from '@typescript-eslint/parser'
import ts_eslint_plugin from '@typescript-eslint/eslint-plugin'

export default [
  {
    ignores: ['dist/**', 'README.md'],
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: ts_parser,
      parserOptions: {
        sourceType: 'module',
        ecmaVersion: 'latest',
      },
    },
    plugins: {
      '@typescript-eslint': ts_eslint_plugin,
    },
    rules: {
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    },
  },
]
