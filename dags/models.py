from sqlalchemy import Column, String
from db import Base, engine


class Song(Base):
    __tablename__ = 'my_played_tracks'
    song_name = Column(String(200))
    artist_name = Column(String(200))
    played_at = Column(String(200), primary_key=True)
    timestamp = Column(String(150))

    def __repr__(self):
        return f'<User {self.song_name} {self.artist_name} {self.played_at}>'


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
