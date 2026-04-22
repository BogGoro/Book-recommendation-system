import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { signIn } from '../api/auth'
import { useAuth } from '../context/AuthContext'

export default function SignIn() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const { setUser } = useAuth()
  const navigate = useNavigate()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const user = await signIn(username.toLowerCase(), password)
      setUser(user)
      navigate('/personal')
    } catch {
      setError('Wrong password or username')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="container">
      <main>
        <div className="content">
          <div
            style={{
              display: 'flex',
              flexDirection: 'row',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: '1.5rem',
              width: '35rem',
            }}
          >
            <h2 style={{ color: 'white', fontSize: '2.2rem' }}>SIGN IN</h2>
            <h4
              style={{ color: 'var(--grey)', textDecoration: 'underline', cursor: 'pointer' }}
              onClick={() => navigate('/registration')}
            >
              SIGN UP
            </h4>
          </div>

          <form onSubmit={handleSubmit}>
            <div className="input-title">Username:</div>
            <div className="tag-box">
              <input
                className="tag"
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter your username"
                required
              />
            </div>
            <br />
            <div className="input-title">Password:</div>
            <div className="tag-box">
              <input
                className="tag"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter your password"
                required
              />
            </div>
            {error && (
              <>
                <br />
                <div className="error">{error}</div>
              </>
            )}
            <br />
            <div className="tag-box">
              <input
                className="tag"
                type="submit"
                value={loading ? 'Logging in...' : 'Log in'}
                disabled={loading}
              />
            </div>
          </form>
        </div>
      </main>

      <spline-viewer
        className="bookshelve"
        url="https://prod.spline.design/6sSB4NA7WSJtxw3F/scene.splinecode"
      />
    </div>
  )
}

