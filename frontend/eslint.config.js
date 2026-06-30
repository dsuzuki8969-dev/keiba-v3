import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { defineConfig, globalIgnores } from 'eslint/config'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    rules: {
      // 日本語テキスト内の全角スペース (U+3000) は意図的に使用
      'no-irregular-whitespace': ['error', {
        skipStrings: true,
        skipRegExps: true,
        skipComments: true,
        skipTemplates: true,
        skipJSXText: true,
      }],
    },
  },
  {
    // design-system プリミティブ / 共有モジュールは variants(cva)・helper を
    // 意図的にコンポーネントと co-locate している。
    // react-refresh/only-export-components は Fast Refresh(開発HMR)専用ルールで
    // 実行時/正当性に影響しないため、これらのファイルでは無効化する。
    files: [
      'src/components/ui/badge.tsx',
      'src/components/ui/button.tsx',
      'src/components/ui/tabs.tsx',
      'src/components/keiba/JikuConfBadge.tsx',
      'src/hooks/useViewMode.tsx',
    ],
    rules: {
      'react-refresh/only-export-components': 'off',
    },
  },
])
