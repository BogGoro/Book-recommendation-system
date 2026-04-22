import { useState, useEffect } from 'react'

let showNotificationFn: ((msg: string) => void) | null = null

export function showNotification(msg: string) {
  showNotificationFn?.(msg)
}

export default function Notification() {
  const [message, setMessage] = useState('')
  const [visible, setVisible] = useState(false)

  useEffect(() => {
    showNotificationFn = (msg: string) => {
      setMessage(msg)
      setVisible(true)
      setTimeout(() => setVisible(false), 2500)
    }
    return () => { showNotificationFn = null }
  }, [])

  return (
    <div
      id="notification"
      style={{ top: visible ? '0' : '-45px' }}
    >
      {message}
    </div>
  )
}
