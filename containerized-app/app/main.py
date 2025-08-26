from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional
import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime

app = FastAPI(
    title="Blog API",
    description="A simple blog API for learning Docker and AWS deployment",
    version="1.0.0"
)

# Database configuration
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/blog.db")

# Pydantic models for request/response
class PostCreate(BaseModel):
    title: str
    content: str
    author: str

class PostUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None

class Post(BaseModel):
    id: int
    title: str
    content: str
    author: str
    created_at: str
    updated_at: str

# Database utilities
@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """Initialize the database with posts table"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data if table is empty
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM posts")
        if cursor.fetchone()[0] == 0:
            sample_posts = [
                ("Welcome to the Blog API", "This is your first blog post created automatically!", "System"),
                ("Docker is Awesome", "Learning to containerize applications with Docker.", "DevOps Learner"),
                ("AWS Deployment", "Taking our containerized app to the cloud!", "Cloud Engineer")
            ]
            
            conn.executemany(
                "INSERT INTO posts (title, content, author) VALUES (?, ?, ?)",
                sample_posts
            )
        
        conn.commit()

# Initialize database on startup
init_db()

# API Routes
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Blog API is running!",
        "version": "1.0.0",
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    """Detailed health check for load balancers"""
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failed: {str(e)}"
        )

@app.get("/posts", response_model=List[Post])
async def get_posts():
    """Get all blog posts"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, title, content, author, created_at, updated_at 
                FROM posts 
                ORDER BY created_at DESC
            """)
            
            posts = []
            for row in cursor.fetchall():
                posts.append(Post(
                    id=row["id"],
                    title=row["title"],
                    content=row["content"],
                    author=row["author"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"]
                ))
            
            return posts
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch posts: {str(e)}"
        )

@app.get("/posts/{post_id}", response_model=Post)
async def get_post(post_id: int):
    """Get a specific blog post by ID"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, title, content, author, created_at, updated_at 
                FROM posts 
                WHERE id = ?
            """, (post_id,))
            
            row = cursor.fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Post with id {post_id} not found"
                )
            
            return Post(
                id=row["id"],
                title=row["title"],
                content=row["content"],
                author=row["author"],
                created_at=row["created_at"],
                updated_at=row["updated_at"]
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch post: {str(e)}"
        )

@app.post("/posts", response_model=Post, status_code=status.HTTP_201_CREATED)
async def create_post(post: PostCreate):
    """Create a new blog post"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO posts (title, content, author)
                VALUES (?, ?, ?)
            """, (post.title, post.content, post.author))
            
            post_id = cursor.lastrowid
            conn.commit()
            
            # Return the created post
            cursor.execute("""
                SELECT id, title, content, author, created_at, updated_at 
                FROM posts 
                WHERE id = ?
            """, (post_id,))
            
            row = cursor.fetchone()
            return Post(
                id=row["id"],
                title=row["title"],
                content=row["content"],
                author=row["author"],
                created_at=row["created_at"],
                updated_at=row["updated_at"]
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create post: {str(e)}"
        )

@app.put("/posts/{post_id}", response_model=Post)
async def update_post(post_id: int, post_update: PostUpdate):
    """Update an existing blog post"""
    try:
        with get_db() as conn:
            # First check if post exists
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM posts WHERE id = ?", (post_id,))
            if not cursor.fetchone():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Post with id {post_id} not found"
                )
            
            # Build dynamic update query based on provided fields
            update_fields = []
            update_values = []
            
            if post_update.title is not None:
                update_fields.append("title = ?")
                update_values.append(post_update.title)
            
            if post_update.content is not None:
                update_fields.append("content = ?")
                update_values.append(post_update.content)
            
            if post_update.author is not None:
                update_fields.append("author = ?")
                update_values.append(post_update.author)
            
            if not update_fields:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="At least one field must be provided for update"
                )
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            update_values.append(post_id)
            
            query = f"UPDATE posts SET {', '.join(update_fields)} WHERE id = ?"
            cursor.execute(query, update_values)
            conn.commit()
            
            # Return updated post
            cursor.execute("""
                SELECT id, title, content, author, created_at, updated_at 
                FROM posts 
                WHERE id = ?
            """, (post_id,))
            
            row = cursor.fetchone()
            return Post(
                id=row["id"],
                title=row["title"],
                content=row["content"],
                author=row["author"],
                created_at=row["created_at"],
                updated_at=row["updated_at"]
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update post: {str(e)}"
        )

@app.delete("/posts/{post_id}")
async def delete_post(post_id: int):
    """Delete a blog post"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM posts WHERE id = ?", (post_id,))
            
            if cursor.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Post with id {post_id} not found"
                )
            
            conn.commit()
            return {"message": f"Post {post_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete post: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)