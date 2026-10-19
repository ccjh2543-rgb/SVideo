import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { decodeJwtPayload, type JwtPayload } from '../utils/jwt'

const ACCESS_KEY = 'access_token'
const REFRESH_KEY = 'refresh_token'
const LEGACY_KEY = 'jwt_token'

function readStorage(key: string): string | null {
  try {
    return localStorage.getItem(key)
  } catch {
    return null
  }
}

function writeStorage(key: string, value: string) {
  localStorage.setItem(key, value)
}

function removeStorage(key: string) {
  localStorage.removeItem(key)
}

export const useAuthStore = defineStore('auth', () => {
  const accessToken = ref<string | null>(readStorage(ACCESS_KEY) || readStorage(LEGACY_KEY))
  const refreshToken = ref<string | null>(readStorage(REFRESH_KEY))

  const token = computed(() => accessToken.value)
  const isLoggedIn = computed(() => !!accessToken.value)
  const claims = computed<JwtPayload | null>(() => (accessToken.value ? decodeJwtPayload(accessToken.value) : null))

  // setTokens 同时保存 Access Token 和 Refresh Token。
  // 旧键 jwt_token 继续写入，兼容尚未迁移的页面和脚本。
  function setTokens(access: string, refresh?: string | null) {
    accessToken.value = access
    writeStorage(ACCESS_KEY, access)
    writeStorage(LEGACY_KEY, access)
    if (refresh) {
      refreshToken.value = refresh
      writeStorage(REFRESH_KEY, refresh)
    }
  }

  // setToken 保留旧调用方式，只更新 Access Token。
  function setToken(newToken: string) {
    setTokens(newToken, refreshToken.value)
  }

  function clearToken() {
    accessToken.value = null
    refreshToken.value = null
    removeStorage(ACCESS_KEY)
    removeStorage(REFRESH_KEY)
    removeStorage(LEGACY_KEY)
  }

  function syncFromStorage() {
    accessToken.value = readStorage(ACCESS_KEY) || readStorage(LEGACY_KEY)
    refreshToken.value = readStorage(REFRESH_KEY)
  }

  return {
    token,
    accessToken,
    refreshToken,
    isLoggedIn,
    claims,
    setToken,
    setTokens,
    clearToken,
    syncFromStorage,
  }
})
