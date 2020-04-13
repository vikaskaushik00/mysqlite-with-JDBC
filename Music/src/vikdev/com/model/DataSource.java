package vikdev.com.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:E:\\Java\\Database\\Music\\" + DB_NAME;

    public static final String TABLE_ALBUM = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTISTS = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTISTS = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTISTS_ID = "_id";
    public static final String COLUMN_ARTISTS_NAME = "name";
    public static final int INDEX_ARTISTS_ID = 1;
    public static final int INDEX_ARTISTS_NAME = 2;

    public static final String TABLE_SONG = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START = "SELECT " + TABLE_ALBUM + '.' + COLUMN_ARTISTS_NAME +
            " FROM " + TABLE_ALBUM + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUM  +
            '.' + COLUMN_ALBUM_ARTISTS + " = " + TABLE_ARTISTS + '.' + COLUMN_ARTISTS_ID +
            " WHERE " + TABLE_ARTISTS + '.' + COLUMN_ARTISTS_NAME + " = \"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUM + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_BY_NAME = " ORDER BY " + COLUMN_ARTISTS_NAME + " COLLATE NOCASE ";

    public static final String QUERY_SONG_BY_NAME = "SELECT " + TABLE_SONG + "." + COLUMN_SONG_TRACK + ", " +
            TABLE_SONG + "." + COLUMN_SONG_TITLE + ", " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_NAME + ", " + TABLE_ALBUM +
            "." + COLUMN_ALBUM_NAME  + " FROM " + TABLE_SONG + " INNER JOIN " + TABLE_ALBUM  + " ON " +
            TABLE_SONG + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUM + "." + COLUMN_ALBUM_ID + " INNER JOIN " +
            TABLE_ARTISTS + " ON " + TABLE_ALBUM + "." + COLUMN_ALBUM_ARTISTS + " = " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_ID +
            " WHERE " + TABLE_SONG + "." + COLUMN_SONG_TITLE + " = ";

    public static final String SORT_SONG_BY_NAME = " ORDER BY " + TABLE_ALBUM + "."+ COLUMN_ALBUM_NAME + " COLLATE NOCASE ";


//CREATE VIEW IF NOT EXISTS music_list AS SELECT songs.track, songs.title, artists.name, albums.name AS album FROM songs
// INNER JOIN albums ON songs.album = albums._id INNER JOIN artists ON albums.artist = artists._id ORDER BY songs.title COLLATE NOCASE  DESC
  public static final String VIEW_NAME = "music_list";

    public static final String VIEW = "CREATE VIEW IF NOT EXISTS " + VIEW_NAME + " AS SELECT " + TABLE_SONG + "." + COLUMN_SONG_TRACK + ", " +
            TABLE_SONG + "." + COLUMN_SONG_TITLE + ", " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_NAME + ", " + TABLE_ALBUM +
            "." + COLUMN_ALBUM_NAME  + " AS album "+ " FROM " + TABLE_SONG + " INNER JOIN " + TABLE_ALBUM  + " ON " +
            TABLE_SONG + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUM + "." + COLUMN_ALBUM_ID + " INNER JOIN " +
            TABLE_ARTISTS + " ON " + TABLE_ALBUM + "." + COLUMN_ALBUM_ARTISTS + " = " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_ID ;


    public static final String SORT_VIEW = " ORDER BY " + TABLE_ARTISTS + "."+ COLUMN_ARTISTS_NAME + " " + TABLE_ALBUM + "." +
            COLUMN_ALBUM_NAME + " " + TABLE_SONG + "." + COLUMN_SONG_TRACK;

    //SELECT name, album, track FROM music_list WHERE title = "Go Your Own Way";

    public static final String QUERY_VIEW_MUSIC_LIST = "SELECT " + COLUMN_SONG_TRACK + ", "+ COLUMN_SONG_TITLE + ", " + COLUMN_ARTISTS_NAME + ", "  +
            COLUMN_SONG_ALBUM + " FROM " + VIEW_NAME + " WHERE " + COLUMN_SONG_TITLE + " = ";

    public static final String QUERY_VIEW_MUSIC_PREP = "SELECT " + COLUMN_SONG_TRACK + ", "+ COLUMN_SONG_TITLE + ", " + COLUMN_ARTISTS_NAME + ", "  +
            COLUMN_SONG_ALBUM + " FROM " + VIEW_NAME + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO  " + TABLE_ARTISTS +
            '(' + COLUMN_ARTISTS_NAME + ')' +" VALUES(?)";

    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUM +
            '(' + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTISTS + ')'+ " VALUES(?, ?)";

    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONG +
            '(' + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM + ')' + " VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTISTS_ID + " FROM " + TABLE_ARTISTS +
            " WHERE " + COLUMN_ARTISTS_NAME + " = ?";

    public static final String QUERY_ALBUMS = " SELECT " + COLUMN_ALBUM_ID + " FROM " + TABLE_ALBUM +
            " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    public static final String QUERY_SONG = "SELECT " + COLUMN_SONG_TITLE + " FROM " + TABLE_SONG +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";

    private PreparedStatement querySongInfo;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement querySongs;

    private Connection conn;

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfo = conn.prepareStatement(QUERY_VIEW_MUSIC_PREP);

            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONGS);

            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUMS);
            querySongs = conn.prepareStatement(QUERY_SONG);

            return true;
        } catch (SQLException e) {
            System.out.println("Something went wrong " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void close() {
        try {
            if (querySongInfo != null){
                querySongInfo.close();
            }


            if (insertIntoArtists != null){
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null){
                insertIntoAlbums.close();
            }
            if (insertIntoSongs != null){
                insertIntoSongs.close();
            }

            if (queryArtist != null){
                queryArtist.close();
            }
            if (queryAlbum != null){
                queryAlbum.close();
            }

            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("Can't close the method " + e.getMessage());
        }
    }

    public List<Artist> artists(int sortOrder) {

        StringBuilder stringBuilder = new StringBuilder("SELECT * FROM ");
        stringBuilder.append(TABLE_ARTISTS);
        if (sortOrder != ORDER_BY_NONE){
            stringBuilder.append(QUERY_ARTIST_BY_NAME);
            if (sortOrder == ORDER_BY_DESC){
                stringBuilder.append(" DESC ");
            }else{
                stringBuilder.append(" ASC ");
            }
        }

        Statement statement = null;
        ResultSet resultSet = null;


        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(stringBuilder.toString());

            List<Artist> artistList = new ArrayList<>();
            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(INDEX_ARTISTS_ID));
                artist.setName(resultSet.getString(INDEX_ARTISTS_NAME));
                artistList.add(artist);
            }
            return artistList;
        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return null;
        } finally {
            try{
                if (resultSet != null){
                    resultSet.close();
                }
            }catch (SQLException e){
                System.out.println("Error while closing ResultSet " + e.getMessage());
            }

            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                System.out.println("Error while closing Statement" + e.getMessage());            }
        }
    }


    public  List<String> queryAlbumForArtist(String artistName, int sortOrder){

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if(sortOrder == ORDER_BY_NONE) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC ");
            } else {
                sb.append("ASC");
            }
        }
        System.out.println("SQL statement = " + sb.toString());
        try(Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(sb.toString())){

            List<String> albums = new ArrayList<>();
            while (result.next()){
                albums.add(result.getString(1));
            }
            return albums;

        }catch (SQLException e){
            e.getMessage();
            return null;
        }
    }

//SELECT songs.track, songs.title, artists.name, albums.name FROM songs INNER JOIN albums ON songs.album = albums._id
// INNER JOIN artists ON albums.artist = artists._id WHERE artists.name = "Carole King" ORDER BY songs.title COLLATE NOCASE ASC



    public List<SongArtist> songArtistList(String artistName, int sortOrder){
        StringBuilder stringBuilder = new StringBuilder(QUERY_SONG_BY_NAME);
        stringBuilder.append("\""+ artistName + "\"");

        if (sortOrder != ORDER_BY_NONE){
            stringBuilder.append(SORT_SONG_BY_NAME);
            if (sortOrder == ORDER_BY_DESC){
                stringBuilder.append(" DESC ");
            }else {
                stringBuilder.append(" ASC ");
            }
        }

        System.out.println("SQL statement for sorting songs : " + stringBuilder.toString());
        try(Statement statement1 = conn.createStatement();
            ResultSet resultSet = statement1.executeQuery(stringBuilder.toString()))
        {
            List<SongArtist> songArtist = new ArrayList<>();
            while (resultSet.next()){
                SongArtist songArtist1 = new SongArtist();
                songArtist1.setTrack(resultSet.getInt(1));
                songArtist1.setSongTitle(resultSet.getString(2));
                songArtist1.setArtistName(resultSet.getString(3));
                songArtist1.setAlbumName(resultSet.getString(4));
                songArtist.add(songArtist1);
            }
            return songArtist;

        }catch (SQLException e){
            e.getMessage();
            return null;
        }

    }

    public void queryMetadata(String tableName){
        String sql = "SELECT * FROM " + tableName;
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql))
        {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int num = resultSetMetaData.getColumnCount();
            for (int i = 1; i <= num; i++){
                System.out.format("Column %d in the song table  name is %s\n",
                        i,resultSetMetaData.getColumnName(i));
            }
        }catch (SQLException e){
            e.getMessage();
        }
    }

    public void countAll(String table){
        String sql = "SELECT COUNT(*) AS count FROM " + table;
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)){

            int count = resultSet.getInt("count");
            System.out.format("Count = %d",count);

        }catch(SQLException e){
            e.getMessage();
        }
    }

    public boolean createView(){

        try(Statement statement = conn.createStatement())
           {
            boolean check = statement.execute(VIEW);
            return check;

        }catch (SQLException e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    public List<SongArtist> queryView(){
        try{
            System.out.println("Enter the song ");
            Scanner scanner = new Scanner(System.in);
            String songName = scanner.nextLine();
            querySongInfo.setString(1,songName);
            ResultSet resultSet3 = querySongInfo.executeQuery();

                List<SongArtist> viewSong = new ArrayList<>();
                while (resultSet3.next()){
                    SongArtist viewObject = new SongArtist();
                    viewObject.setTrack(resultSet3.getInt(1));
                    viewObject.setSongTitle(resultSet3.getString(2));
                    viewObject.setArtistName(resultSet3.getString(3));
                    viewObject.setAlbumName(resultSet3.getString(4));
                    viewSong.add(viewObject);
                }
                return  viewSong;
            }catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;

        }
    }


    private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1,name);
        ResultSet resultSet = queryArtist.executeQuery();
        if(resultSet.next()){
            return resultSet.getInt(1);
        }else {
            //Insert artists
            insertIntoArtists.setString(1,name);

            int rowsAffected = insertIntoArtists.executeUpdate();
            if (rowsAffected != 1){
                throw new SQLException("Couldn't insert artist");
            }

            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get the keys for artist ");
            }
        }
    }


    private int insertAlbum(String name, int artistId) throws SQLException{
        queryAlbum.setString(1,name);
        ResultSet resultSet = queryAlbum.executeQuery();
        if(resultSet.next()){
            return resultSet.getInt(1);
        }else {
            //Insert albums
            insertIntoAlbums.setString(1,name);
            insertIntoAlbums.setInt(2,artistId);
            int rowsAffected = insertIntoAlbums.executeUpdate();
            if (rowsAffected != 1){
                throw new SQLException("Couldn't insert albums");
            }

            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get the keys for albums ");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {
        try{
            conn.setAutoCommit(false);

            querySongs.setString(1,title);
            ResultSet resultSet = querySongs.executeQuery();
            if (resultSet.next()){
                throw new SQLException("Already exists");
            }else {

                int artistId = insertArtist(artist);
                int albumId = insertAlbum(album, artistId);
                insertIntoSongs.setInt(1, track);
                insertIntoSongs.setString(2, title);
                insertIntoSongs.setInt(3, albumId);

                int rowsAffected = insertIntoSongs.executeUpdate();

                if (rowsAffected == 1) {
                    conn.commit();
                } else {
                    throw new SQLException("The song insert is failed");
                }
            }
        }catch (Exception e){
            System.out.println("Couldn't insert song " + e.getMessage());
            try{
                System.out.println("Performing rollback");
                conn.rollback();
            }catch (SQLException e2){
                System.out.println("Unable to rollback" + e2.getMessage());
            }
        }
        finally {
            try{
                System.out.println("Resetting the auto commit ");
                conn.setAutoCommit(true);
            }catch (SQLException sq){
                System.out.println("Couldn't resetting the auto commit ");
            }
        }
    }
}
