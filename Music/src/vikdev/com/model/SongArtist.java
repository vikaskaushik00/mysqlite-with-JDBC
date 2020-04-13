package vikdev.com.model;

public class SongArtist {
    private int track;
    private String songTitle;
    private String artistName;
    private String albumName;

    public int getTrack() {
        return track;
    }

    public void setTrack(int track) {
        this.track = track;
    }

    public String getSongTitle() {
        return songTitle;
    }

    public void setSongTitle(String songTitle) {
        this.songTitle = songTitle;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getAlbumName() {
        return albumName;
    }

    public void setAlbumName(String albumName) {
        this.albumName = albumName;
    }
    public String toString(){
        return "Track -> "+track + " | " +" Song -> "+ songTitle + " | " +" Artist -> "+ artistName + " | " +"Album -> "+ albumName;
    }
}
