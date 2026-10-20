import { defineStore } from 'pinia'
import { ref } from 'vue'

import { useAuthStore } from './auth'
import { useToastStore } from './toast'

export type NotificationItem = {
  event_id?: string
  type: string
  title: string
  body: string
  data?: Record<string, unknown>
}

const MAX_ITEMS = 50
const RECONNECT_DELAY = 3000

export const useNotificationStore = defineStore('notifications', () => {
  const connected = ref(false)
  const items = ref<NotificationItem[]>([])
  const seen = new Set<string>()

  const auth = useAuthStore()
  const toast = useToastStore()

  let socket: WebSocket | null = null
  let reconnectTimer: number | null = null
  let manualClose = false

  function connect(token: string) {
    if (!token) return
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
      return
    }
    manualClose = false
    clearReconnectTimer()

    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const url = `${protocol}://${window.location.host}/ws?token=${encodeURIComponent(token)}`
    socket = new WebSocket(url)

    socket.onopen = () => {
      connected.value = true
    }
    socket.onmessage = (event) => {
      handleMessage(event.data)
    }
    socket.onclose = () => {
      connected.value = false
      socket = null
      if (!manualClose && auth.isLoggedIn && auth.token) {
        reconnectTimer = window.setTimeout(() => connect(auth.token ?? ''), RECONNECT_DELAY)
      }
    }
    socket.onerror = () => {
      socket?.close()
    }
  }

  function disconnect() {
    manualClose = true
    clearReconnectTimer()
    socket?.close()
    socket = null
    connected.value = false
  }

  function handleMessage(raw: unknown) {
    if (typeof raw !== 'string') return
    let item: NotificationItem
    try {
      item = JSON.parse(raw) as NotificationItem
    } catch {
      return
    }
    if (!item || !item.title) return
    if (item.event_id) {
      if (seen.has(item.event_id)) return
      seen.add(item.event_id)
    }

    items.value.unshift(item)
    if (items.value.length > MAX_ITEMS) {
      items.value = items.value.slice(0, MAX_ITEMS)
    }
    toast.info(`${item.title}：${item.body}`)
  }

  function clearReconnectTimer() {
    if (reconnectTimer !== null) {
      window.clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  return { connected, items, connect, disconnect }
})
