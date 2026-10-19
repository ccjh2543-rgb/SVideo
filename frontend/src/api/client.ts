import { useAuthStore } from '../stores/auth'
import type { TokenResponse } from './types'

export class ApiError extends Error {
  status: number
  payload?: unknown

  constructor(message: string, status: number, payload?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.payload = payload
  }
}

type ApiErrorBody = { error?: string }

type RequestOptions = {
  authRequired?: boolean
  // 登录、注册、刷新等接口失败时不要清理本地会话
  clearOn401?: boolean
  // 刷新请求本身不再触发自动刷新，避免递归
  skipRefresh?: boolean
}

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api'

// 全局只允许一个刷新请求在飞行，避免并发 401 触发多次刷新。
let refreshPromise: Promise<string | null> | null = null

async function refreshAccessToken(): Promise<string | null> {
  const auth = useAuthStore()
  const refreshToken = auth.refreshToken
  if (!refreshToken) return null

  if (refreshPromise) return refreshPromise

  refreshPromise = (async () => {
    try {
      const res = await fetch(`${API_BASE}/account/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
      })
      if (!res.ok) return null

      const data = (await res.json()) as TokenResponse
      const accessToken = data.access_token || data.token
      if (!accessToken || !data.refresh_token) return null

      auth.setTokens(accessToken, data.refresh_token)
      return accessToken
    } catch {
      return null
    } finally {
      refreshPromise = null
    }
  })()

  return refreshPromise
}

async function request<T>(path: string, init: RequestInit, options?: RequestOptions): Promise<T> {
  const auth = useAuthStore()
  const token = auth.token

  if (options?.authRequired && !token) {
    throw new ApiError('需要先登录（缺少 token）', 401)
  }

  const headers = new Headers(init.headers)
  if (token) headers.set('Authorization', `Bearer ${token}`)

  let res = await fetch(`${API_BASE}${path}`, { ...init, headers })

  // Access Token 过期时自动刷新一次，然后重放原请求。
  if (res.status === 401 && token && auth.refreshToken && !options?.skipRefresh) {
    const newAccessToken = await refreshAccessToken()
    if (newAccessToken) {
      headers.set('Authorization', `Bearer ${newAccessToken}`)
      res = await fetch(`${API_BASE}${path}`, { ...init, headers })
    }
  }

  const text = await res.text()
  let data: unknown = null
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = text
    }
  }

  if (!res.ok) {
    if (res.status === 401 && options?.clearOn401 !== false) {
      auth.clearToken()
    }
    const msg =
      data && typeof data === 'object' && (data as ApiErrorBody).error
        ? String((data as ApiErrorBody).error)
        : `请求失败 (${res.status})`
    throw new ApiError(msg, res.status, data)
  }

  return data as T
}

export async function postJson<T>(path: string, body: unknown, options?: RequestOptions): Promise<T> {
  return request<T>(
    path,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    },
    options,
  )
}

export async function postForm<T>(path: string, body: FormData, options?: RequestOptions): Promise<T> {
  return request<T>(path, { method: 'POST', body }, options)
}
