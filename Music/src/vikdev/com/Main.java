package vikdev.com;

import vikdev.com.model.Artist;
import vikdev.com.model.DataSource;
import vikdev.com.model.SongArtist;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        if (!dataSource.open()){
            System.out.println("Can't open dataSource");
            return;
        }
//        List<Artist> artists = dataSource.artists(1);
//        if (artists == null){
//            System.out.println("No artists");
//            return;
//        }
//        for (Artist artistList : artists){
//            System.out.println("ID : " + artistList.getId() + " -> Name : " + artistList.getName());
//        }
//
//        List<String> albumForArtist = dataSource.queryAlbumForArtist("Carole King", DataSource.ORDER_BY_DESC);
//        for (String name : albumForArtist){
//            System.out.println(name);
//        }

//        List<SongArtist> allSongs = dataSource.songArtistList("Go Your Own Way",DataSource.ORDER_BY_ASC);
//        if (allSongs == null){
//            System.out.println("No query in the list ");
//            return;
//        }
//            for (SongArtist songArtist : allSongs){
//            System.out.println(songArtist);
//        }
//            dataSource.queryMetadata(DataSource.TABLE_ARTISTS);

      //  dataSource.countAll(DataSource.TABLE_SONG);
//        if (!dataSource.createView()){
//            System.out.println("View created successfully by name " + DataSource.VIEW_NAME);
//        }else {
//            System.out.println("View creation failed ");
//        }

//        List<SongArtist> listView = dataSource.queryView();
//        if (!listView.isEmpty()){
//            for (SongArtist songArtist : listView){
//                System.out.println(songArtist.toString());
//            }
//        }else {
//            System.out.println("couldn't find the song");
//        }

//        dataSource.insertSong("Jannat","B Prak","Sad song",10);
//        dataSource.insertSong("Jannat","B Prak","Sad song",10);
       // dataSource.insertSong("Filhal","B Prak","Sad song",17);
        dataSource.insertSong("Filhal","B Prak","Sad song",17);
        dataSource.close();
    }
}
