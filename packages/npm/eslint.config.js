import js from '@eslint/js'
import tseslint from 'typescript-eslint'
import unicorn from 'eslint-plugin-unicorn'
import globals from 'globals'

export default [
  js.configs.recommended,
  ...tseslint.configs.recommended,
  unicorn.configs['flat/recommended'],
  {
    rules: {
      'unicorn/no-null': 'off',
      'unicorn/prevent-abbreviations': 'off',
      'unicorn/prefer-top-level-await': 'off',
      'unicorn/no-this-assignment': 'off',
      // Prettier lowercases hex literals; align unicorn with it.
      'unicorn/number-literal-case': ['error', { hexadecimalValue: 'lowercase' }],
    },
  },
  {
    files: ['src/**/*.js', 'src/**/*.ts'],
    languageOptions: {
      globals: globals.browser,
    },
  },
  {
    files: ['src/node.ts', 'src/wasm-dir.ts', 'src/copy-wasm-extensions.ts'],
    languageOptions: {
      globals: globals.node,
    },
  },
  {
    files: ['src/__tests__/**/*.js'],
    languageOptions: {
      globals: {
        ...globals.node,
        Bun: 'readonly',
      },
    },
  },
]
