import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import type { UserData } from '../types'
import { getMe, logout as apiLogout } from '../api/auth'

interface AuthContextValue {
  user: UserData | null
  loading: boolean
  setUser: (u: UserData | null) => void
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue>({
  user: null,
  loading: true,
  setUser: () => {},
  logout: async () => {},
})

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<UserData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getMe().then((u) => {
      setUser(u)
      setLoading(false)
    })
  }, [])

  async function logout() {
    await apiLogout()
    setUser(null)
  }

  return (
    <AuthContext.Provider value={{ user, loading, setUser, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  return useContext(AuthContext)
}
