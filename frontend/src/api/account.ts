import { postJson } from './client'
import type { Account, MessageResponse, TokenResponse } from './types'

export function register(username: string, password: string) {
  return postJson<MessageResponse>(
    '/account/register',
    { username, password },
    { clearOn401: false, skipRefresh: true },
  )
}

export function login(username: string, password: string) {
  return postJson<TokenResponse>(
    '/account/login',
    { username, password },
    { clearOn401: false, skipRefresh: true },
  )
}

export function logout(refreshToken: string) {
  return postJson<MessageResponse>(
    '/account/logout',
    { refresh_token: refreshToken },
    { authRequired: true },
  )
}

export function rename(newUsername: string) {
  return postJson<TokenResponse>('/account/rename', { new_username: newUsername }, { authRequired: true })
}

export function changePassword(oldPassword: string, newPassword: string) {
  return postJson<MessageResponse>(
    '/account/changePassword',
    { old_password: oldPassword, new_password: newPassword },
    { authRequired: true },
  )
}

export function findById(id: number) {
  return postJson<Account>('/account/findByID', { id })
}

export function findByUsername(username: string) {
  return postJson<Account>('/account/findByUsername', { username })
}
