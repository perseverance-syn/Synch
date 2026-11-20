// Renderer entry: mounts the app with one TanStack Query client for all API reads.
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App } from './App'
import './styles.css'

const queries = new QueryClient({ defaultOptions: { queries: { refetchOnWindowFocus: false } } })

createRoot(document.getElementById('root')!).render(
  <QueryClientProvider client={queries}>
    <App />
  </QueryClientProvider>,
)
