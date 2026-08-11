import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const target = env.GOAI_API_TARGET || 'http://localhost:8080'

  return {
    plugins: [react()],
    server: {
      host: '127.0.0.1',
      port: 5173,
      proxy: Object.fromEntries(
        ['/api', '/auth', '/a2a', '/ping'].map((path) => [path, { target, changeOrigin: true }]),
      ),
    },
  }
})
