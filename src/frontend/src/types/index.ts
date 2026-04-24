export interface Book {
  id: number
  title: string
  author: string
  cover: string
  description?: string
}

export interface BookStats {
  statuses: [number, number, number]
  scores: [number, number, number, number, number, number]
}

export interface Comment {
  author: string
  text: string
}

export interface UserData {
  preferred_username: string
}

export interface UserBookEntry {
  book: Book
  score: number | null
}

export interface UserStats {
  scores: [number, number, number, number, number]
}

export type ReadingStatus = 'completed' | 'reading' | 'planned' | 'untracked'
