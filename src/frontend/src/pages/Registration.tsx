import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { register } from '../api/auth'
import { useAuth } from '../context/AuthContext'

export default function Registration() {
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [passwordConfirm, setPasswordConfirm] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const { setUser } = useAuth()
  const navigate = useNavigate()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')

    if (password !== passwordConfirm) {
      setError("Passwords don't match")
      return
    }

    setLoading(true)
    try {
      const user = await register(username.toLowerCase(), email, password)
      setUser(user)
      navigate('/personal')
    } catch (err: any) {
      setError(err?.response?.data?.detail || 'This username is already taken, try another one')
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
            <h2 style={{ color: 'white', fontSize: '2.2rem' }}>SIGN UP</h2>
            <h4
              style={{ color: 'var(--grey)', textDecoration: 'underline', cursor: 'pointer' }}
              onClick={() => navigate('/signin')}
            >
              SIGN IN
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
            <div className="input-title">Email:</div>
            <div className="tag-box">
              <input
                className="tag"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Enter your email"
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
            <br />
            <div className="input-title">Confirm password:</div>
            <div className="tag-box">
              <input
                className="tag"
                type="password"
                value={passwordConfirm}
                onChange={(e) => setPasswordConfirm(e.target.value)}
                placeholder="Confirm your password"
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
                value={loading ? 'Creating account...' : 'Sign Up'}
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

