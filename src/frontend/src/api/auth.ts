import client from './client'
import type { UserData } from '../types'

export async function getMe(): Promise<UserData | null> {
  try {
    const res = await client.get<UserData>('/me')
    return res.data
  } catch {
    return null
  }
}

export async function signIn(username: string, password: string): Promise<UserData> {
  const res = await client.post<UserData>('/signin', { username, password })
  return res.data
}

export async function register(
  username: string,
  email: string,
  password: string
): Promise<UserData> {
  const res = await client.post<UserData>('/registration', { username, email, password })
  return res.data
}

export async function logout(): Promise<void> {
  await client.post('/logout')
}
