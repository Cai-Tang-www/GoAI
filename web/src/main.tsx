import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App as AntApp, ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { AuthProvider } from './auth/AuthContext'
import './styles.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false },
  },
})
window.addEventListener('goai:logout', () => queryClient.clear())

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider
      locale={zhCN}
      theme={{
        token: {
          colorPrimary: '#286f6c',
          colorInfo: '#286f6c',
          colorSuccess: '#2f7b4c',
          colorWarning: '#b06b20',
          colorError: '#b7463c',
          colorText: '#1c292f',
          colorTextSecondary: '#66757a',
          colorBorder: '#d9e0df',
          colorBgLayout: '#f3f5f5',
          borderRadius: 4,
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
          controlHeight: 36,
        },
        components: {
          Button: { fontWeight: 600 },
          Menu: { itemBorderRadius: 3, itemHeight: 42 },
          Table: { headerBg: '#f6f8f8', headerColor: '#536468' },
          Modal: { borderRadiusLG: 6 },
          Tag: { borderRadiusSM: 2 },
        },
      }}
    >
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter><AuthProvider><App /></AuthProvider></BrowserRouter>
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  </React.StrictMode>,
)
