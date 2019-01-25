/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ceng.ceng351.musicdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lenovo
 */
class MUSICDB implements IMUSICDB{
    
    private static Connection conn = null;
    
    @Override
    public void initialize()
    {
    
        try{
            String url = "jdbc:mysql://144.122.71.57:8084/db2171254";
            String username = "e2171254";
            String password = "ab260624";
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } 
    }
    
    @Override
    public int createTables() {
        Connection dbConnection = null;
        Statement statement = null;
        Statement statement2 = null;
        String mystr = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'db2171254'";
        int tablecount = 0;
        String user = "CREATE TABLE user("
				+ "userID int NOT NULL, "
				+ "userName varchar(60), "
				+ "email varchar(30), "
				+ "password varchar(30), " 
                                + "PRIMARY KEY (userID) "
				+ ")";
        String artist = "CREATE TABLE artist("
				+ "artistID int NOT NULL, "
				+ "artistName varchar(60), "
				+ "PRIMARY KEY (artistID) "
				+ ")";
        String album = "CREATE TABLE album("
				+ "albumID int NOT NULL, "
				+ "title varchar(60), "
				+ "albumGenre varchar(30), "
				+ "albumRating double, "
                                + "releaseDate date,"
                                + "artistID int,"
                                + "PRIMARY KEY (albumID),"
                                + "FOREIGN KEY (artistID) REFERENCES artist(artistID) ON UPDATE CASCADE ON DELETE CASCADE "
				+ ")";
        
        String song = "CREATE TABLE song("
				+ "songID int NOT NULL, "
				+ "songName varchar(60), "
				+ "genre varchar(30), "
				+ "rating double, "
                                + "artistID int,"
                                + "albumID int,"
                                + "PRIMARY KEY (songID),"
                                + "FOREIGN KEY (artistID) REFERENCES artist(artistID) ON DELETE CASCADE ON UPDATE CASCADE,"
                                + "FOREIGN KEY (albumID) REFERENCES album(albumID) ON DELETE CASCADE ON UPDATE CASCADE"
				+ ")";
       
        String listen = "CREATE TABLE listen("
				+ "userID int NOT NULL, "
				+ "songID int NOT NULL, "
				+ "lastListenTime timestamp,"
                                + "listenCount int,"
                                + "PRIMARY KEY (userID,songID),"
                                + "FOREIGN KEY (userID) REFERENCES user(userID) ON DELETE CASCADE ON UPDATE CASCADE,"
                                + "FOREIGN KEY (songID) REFERENCES song(songID) ON DELETE CASCADE ON UPDATE CASCADE"
				+ ")";
        
                try {
			dbConnection = conn;
                        if(dbConnection != null)
                        {
                            statement = dbConnection.createStatement();
                            statement2 = dbConnection.createStatement();
                            statement.execute(user);
                            statement.execute(artist);
                            statement.execute(album);
                            statement.execute(song);
                            statement.execute(listen);
                            ResultSet result = statement2.executeQuery(mystr);
                            result.next();
                            tablecount = result.getInt(1);
                            return tablecount;
                        }


		}
                catch (SQLException e) {
			System.out.println(e.getMessage());
		}
                finally {
			if (statement != null) {
                            try {
                                statement.close();
                            } catch (SQLException ex) {
                                Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                            }
			}
		}
        return tablecount;
    }
    

    @Override
    public int dropTables() {
        Connection dbConnection = null;
        Statement statement = null;
        Statement statement2 = null;

        int countbefore = 0;
        int countafter = 0;
        String str1 = "SET FOREIGN_KEY_CHECKS = 0";
        String user = "drop table if exists user";
        String artist = "drop table if exists artist";
        String album = "drop table if exists album";
        String song = "drop table if exists song";
        String listen = "drop table if exists listen";
        String str2  = "SET FOREIGN_KEY_CHECKS = 1";
         try {
			dbConnection = conn;
                        if(dbConnection != null)
                        {   statement2 = dbConnection.createStatement();
                            ResultSet resultSet1 = statement2.executeQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'db2171254'");
                            resultSet1.next();
                            countbefore = resultSet1.getInt(1);
                            statement = dbConnection.createStatement();
                            statement.execute(str1);
                            statement.execute(user);
                            statement.execute(artist);
                            statement.execute(album);
                            statement.execute(song);
                            statement.execute(listen);
                            statement.execute(str2);
                            ResultSet resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'db2171254'");                                           
                            resultSet2.next();
                            countafter = resultSet2.getInt(1);
                            return countbefore-countafter;
                        }

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (statement != null) {
                            try {
                                statement.close();
                            } catch (SQLException ex) {
                                Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                            }
			}
		}
         return countbefore-countafter;
      }

    @Override
    public int insertAlbum(Album[] albums) {
        int rowcount = 0;
        try {
            String query =  " insert into album (albumID, title, albumGenre, albumRating, releaseDate, artistID)"
                    + " values (?, ?, ?, ?, ?,?)";
            for(int i = 0;i<albums.length;i++)
            {
                try {
                    Statement statement = conn.createStatement();
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0");
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt    (1, albums[i].getAlbumID());
                    preparedStmt.setString (2, albums[i].getTitle());
                    preparedStmt.setString (3, albums[i].getAlbumGenre());
                    preparedStmt.setDouble (4, albums[i].getAlbumRating());
                    preparedStmt.setString (5, albums[i].getReleaseDate());
                    preparedStmt.setInt    (6, albums[i].getArtistID());
                    preparedStmt.executeUpdate();
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1");
                    
                } catch (SQLException ex) {
                    Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM album");
            resultSet.next();
            rowcount = resultSet.getInt(1);
            }
        catch (SQLException ex) {
         Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rowcount;
    }
    

    @Override
    public int insertArtist(Artist[] artists) {
        int rowcount = 0;
        try {
            String query =  " insert into artist (artistID, artistName)"
                    + " values (?, ?)";
            for(int i = 0;i<artists.length;i++)
            {
                try {
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt    (1, artists[i].getArtistID());
                    preparedStmt.setString (2, artists[i].getArtistName());
                    preparedStmt.executeUpdate();
                } catch (SQLException ex) {
                    Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM artist");
            resultSet.next();
            rowcount = resultSet.getInt(1);
            }
        catch (SQLException ex) {
         Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rowcount;
    }

    @Override
    public int insertSong(Song[] songs) {
        int rowcount = 0;
        try {
            String query =  " insert into song (songID, songName, genre, rating, artistID, albumID)"
                    + " values (?, ?, ?, ?, ?,?)";
            for(int i = 0;i<songs.length;i++)
            {
                try {
                    Statement statement = conn.createStatement();
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0");
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt    (1, songs[i].getSongID());
                    preparedStmt.setString (2, songs[i].getSongName());
                    preparedStmt.setString (3, songs[i].getGenre());
                    preparedStmt.setDouble (4, songs[i].getRating());
                    preparedStmt.setInt    (5, songs[i].getArtistID());
                    preparedStmt.setInt    (6, songs[i].getAlbumID());
                    preparedStmt.executeUpdate();
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1");
                } catch (SQLException ex) {
                    Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM song");
            resultSet.next();
            rowcount = resultSet.getInt(1);
            }
        catch (SQLException ex) {
         Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rowcount;
    }

    @Override
    public int  insertUser(User[] users) {
        int rowcount = 0;
        try {
            String query =  " insert into user (userID, userName, email, password)"
                    + " values (?, ?, ?, ?)";
            for(int i = 0;i<users.length;i++)
            {
                try {
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt    (1, users[i].getUserID());
                    preparedStmt.setString (2, users[i].getUserName());
                    preparedStmt.setString (3, users[i].getEmail());
                    preparedStmt.setString (4, users[i].getPassword());
                    preparedStmt.executeUpdate();

                } catch (SQLException ex) {
                    Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user;");
                while(rs.next())
                {
                rowcount = rs.getInt("count(*)");
                }
            }
        catch (SQLException ex) {
         Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rowcount;    
    }

    @Override
    public int insertListen(Listen[] listens) {
        int rowcount = 0;
        try {
            String query =  " insert into listen (userID, songID, lastListenTime, listenCount)"
                    + " values (?, ?, ?, ?)";
            for(int i = 0;i<listens.length;i++)
            {
                try {
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt       (1, listens[i].getUserID());
                    preparedStmt.setInt       (2, listens[i].getSongID());
                    preparedStmt.setTimestamp (3, listens[i].getLastListenTime());
                    preparedStmt.setInt       (4, listens[i].getListenCount());
                    preparedStmt.executeUpdate();
                } catch (SQLException ex) {
                    Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM listen");
            resultSet.next();
            rowcount = resultSet.getInt(1);
            }
        catch (SQLException ex) {
         Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rowcount; 
    }

    @Override
    public QueryResult.ArtistNameSongNameGenreRatingResult[] getHighestRatedSongs() {
        try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();

            ResultSet resultSet = stmt1.executeQuery("SELECT DISTINCT A.artistName,S.songName,S.genre,S.rating"
                                                  +" FROM artist A,song S"
                                                  +" WHERE A.artistID = S.artistID AND S.rating = (SELECT max(S2.rating) FROM song S2)"
                                                  +" ORDER BY A.artistName");
            
            ResultSet resultSet2 = stmt2.executeQuery("SELECT COUNT(*)"
                                                  +" FROM artist A,song S"
                                                  +" WHERE A.artistID = S.artistID AND S.rating = (SELECT max(S2.rating) FROM song S2)");
            resultSet2.next();
            count = resultSet2.getInt(1);
            resultSet.next();
            QueryResult.ArtistNameSongNameGenreRatingResult[] resultarray = new QueryResult.ArtistNameSongNameGenreRatingResult[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new QueryResult.ArtistNameSongNameGenreRatingResult(resultSet.getString(1),resultSet.getString(2),resultSet.getString(3),resultSet.getDouble(4));
                resultSet.next();
            }
            return resultarray;
        } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public QueryResult.TitleReleaseDateRatingResult getMostRecentAlbum(String artistName) {
        try {
            Statement stmt1 = conn.createStatement();

            ResultSet resultSet = stmt1.executeQuery("SELECT DISTINCT A.title,A.releaseDate,A.albumRating"
                                                  +" FROM album A"
                                                  +" WHERE A.releaseDate = (SELECT max(A2.releaseDate) FROM album A2,artist ART"
                                                                           +" WHERE ART.artistName = "+ "'"+artistName+"'"
                                                                           +" AND A2.artistID = ART.artistID)"); 

            resultSet.next();
            QueryResult.TitleReleaseDateRatingResult result = new QueryResult.TitleReleaseDateRatingResult(resultSet.getString(1),resultSet.getString(2),resultSet.getDouble(3));
            return result;
        } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public QueryResult.ArtistNameSongNameGenreRatingResult[] getCommonSongs(String userName1, String userName2) 
    {
    try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();

            ResultSet resultSet = stmt1.executeQuery("SELECT DISTINCT artist1.artistName,song1.songName,song1.genre,song1.rating"
                                                  +" FROM user as user1,listen as listen1,song as song1,artist as artist1"
                                                  +" WHERE user1.userName = " + "'"+userName1+"'" + " AND user1.userID = listen1.userID"
                                                  +" AND listen1.songID = song1.songID AND song1.artistID = artist1.artistID AND EXISTS"
                                                  +" (SELECT DISTINCT artist2.artistName,song2.songName,song2.genre,song2.rating"
                                                  +" FROM user as user2,listen as listen2,song as song2,artist as artist2"
                                                  +" WHERE user2.userName = " + "'"+userName2+"'" + " AND user2.userID = listen2.userID"
                                                  +" AND listen2.songID = song2.songID AND song2.artistID = artist2.artistID"
                                                  +" AND artist1.artistName = artist2.artistName"
                                                  +" AND song1.songName = song2.songName"
                                                  +" AND song1.genre = song2.genre "
                                                  +" AND song1.rating = song2.rating ORDER BY song1.rating)");
                                                  
                    
            ResultSet resultSet2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                  +" (SELECT DISTINCT artist1.artistName,song1.songName,song1.genre,song1.rating"
                                                  +" FROM user as user1,listen as listen1,song as song1,artist as artist1"
                                                  +" WHERE user1.userName = " + "'"+userName1+"'" + " AND user1.userID = listen1.userID"
                                                  +" AND listen1.songID = song1.songID AND song1.artistID = artist1.artistID AND EXISTS"
                                                  +" (SELECT DISTINCT artist2.artistName,song2.songName,song2.genre,song2.rating"
                                                  +" FROM user as user2,listen as listen2,song as song2,artist as artist2"
                                                  +" WHERE user2.userName = " + "'"+userName2+"'" + " AND user2.userID = listen2.userID"
                                                  +" AND listen2.songID = song2.songID AND song2.artistID = artist2.artistID"
                                                  +" AND artist1.artistName = artist2.artistName"
                                                  +" AND song1.songName = song2.songName"
                                                  +" AND song1.genre = song2.genre "
                                                  +" AND song1.rating = song2.rating)) AS S");
            resultSet2.next();
            count = resultSet2.getInt(1);
            resultSet.next();
            QueryResult.ArtistNameSongNameGenreRatingResult[] resultarray = new QueryResult.ArtistNameSongNameGenreRatingResult[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new QueryResult.ArtistNameSongNameGenreRatingResult(resultSet.getString(1),resultSet.getString(2),resultSet.getString(3),resultSet.getDouble(4));
                resultSet.next();
            }
            return resultarray;
        } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public QueryResult.ArtistNameNumberOfSongsResult[] getNumberOfTimesSongsListenedByUser(String userName) {
    try {
        int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();

            ResultSet resultSet = stmt1.executeQuery("SELECT DISTINCT artist1.artistName,sum(listen1.listenCount)"
                                                  +" FROM user as user1,listen as listen1,song as song1,artist as artist1"
                                                  +" WHERE user1.userName = " + "'"+userName+"'" + " AND user1.userID = listen1.userID"
                                                  +" AND listen1.songID = song1.songID AND song1.artistID = artist1.artistID"
                                                  +" GROUP BY artist1.artistName"
                                                  +" ORDER BY artist1.artistName");
                                                  
                    
            ResultSet resultSet2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                  +" (SELECT DISTINCT artist1.artistName,sum(listen1.listenCount)"
                                                  +" FROM user as user1,listen as listen1,song as song1,artist as artist1"
                                                  +" WHERE user1.userName = " + "'"+userName+"'" + " AND user1.userID = listen1.userID"
                                                  +" AND listen1.songID = song1.songID AND song1.artistID = artist1.artistID"
                                                  +" GROUP BY artist1.artistName) AS S");
            
            resultSet2.next();
            count = resultSet2.getInt(1);
            resultSet.next();
            QueryResult.ArtistNameNumberOfSongsResult[] resultarray = new QueryResult.ArtistNameNumberOfSongsResult[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new QueryResult.ArtistNameNumberOfSongsResult(resultSet.getString(1),resultSet.getInt(2));
                resultSet.next();
            }
            return resultarray;
            } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public User[] getUsersWhoListenedAllSongs(String artistName) {
       try {
        int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();

            ResultSet resultSet = stmt1.executeQuery("SELECT DISTINCT *"
                                                  +" FROM user U"
                                                  +" WHERE EXISTS(SELECT A.artistID"
                                                  +"                  FROM artist A"
                                                  +"                  WHERE A.artistName = " + "'"+artistName+"'"
                                                  +"                  AND NOT EXISTS(SELECT S.songID"
                                                  +"                                 FROM song S"
                                                  +"                                 WHERE S.artistID = A.artistID"
                                                  +"                                 AND NOT EXISTS(SELECT L.songID"
                                                  +"                                                FROM listen L"
                                                  +"                                                WHERE L.songID = S.songID"
                                                  +"                                                AND L.userID = U.userID)))"
                                                  +" ORDER BY U.userID");
                                                  
                    
            ResultSet resultSet2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                  +" (SELECT DISTINCT * "
                                                  +" FROM user U"
                                                  +" WHERE EXISTS(SELECT A.artistID"
                                                  +"                  FROM artist A"
                                                  +"                  WHERE A.artistName = " + "'"+artistName+"'"
                                                  +"                  AND NOT EXISTS(SELECT S.songID"
                                                  +"                                 FROM song S"
                                                  +"                                 WHERE S.artistID = A.artistID"
                                                  +"                                 AND NOT EXISTS(SELECT L.songID"
                                                  +"                                                FROM listen L"
                                                  +"                                                WHERE L.songID = S.songID"
                                                  +"                                                AND L.userID = U.userID)))) AS S");
            
            resultSet2.next();
            count = resultSet2.getInt(1);
            resultSet.next();
            User[] resultarray = new User[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new User(resultSet.getInt(1),resultSet.getString(2),resultSet.getString(3),resultSet.getString(4));
                resultSet.next();
            }
            return resultarray;
            } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public QueryResult.UserIdUserNameNumberOfSongsResult[] getUserIDUserNameNumberOfSongsNotListenedByAnyone() {
                    try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            ResultSet resultset = stmt1.executeQuery(" SELECT DISTINCT U.userID,U.userName,count(L.songID)"
                                                    +" FROM user U,listen L"
                                                    +" WHERE U.userID = L.userID AND L.songID NOT IN"
                                                    +"                                              (SELECT DISTINCT L2.songID"
                                                    +"                                               FROM user U2,listen L2"
                                                    +"                                               WHERE U2.userID = L2.userID AND L2.userID != L.userID)"
                                                    +" GROUP BY L.userID"
                                                    +" ORDER BY U.userID");
                    

                    
            ResultSet resultset2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                    +" (SELECT DISTINCT U.userID,U.userName,count(L.userID)"
                                                    +" FROM user U,listen L"
                                                    +" WHERE U.userID = L.userID AND L.songID NOT IN"
                                                    +"                                              (SELECT DISTINCT L2.songID"
                                                    +"                                               FROM user U2,listen L2"
                                                    +"                                               WHERE U2.userID = L2.userID AND L2.userID != L.userID)"
                                                    +" GROUP BY L.userID) as S");

            resultset2.next();
            count = resultset2.getInt(1);
            resultset.next();
            QueryResult.UserIdUserNameNumberOfSongsResult[] resultarray = new QueryResult.UserIdUserNameNumberOfSongsResult[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new QueryResult.UserIdUserNameNumberOfSongsResult(resultset.getInt(1),resultset.getString(2),resultset.getInt(3));
                resultset.next();
            }
            return resultarray;
        } catch (SQLException ex) {
           Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public Artist[] getArtistSingingPopGreaterAverageRating(double rating) {
            try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            ResultSet resultset = stmt1.executeQuery(" SELECT DISTINCT A.artistID,A.artistName"
                                                    +" FROM artist A,song S"
                                                    +" WHERE A.artistID = S.artistID AND A.artistID IN"
                                                    +"                                               (SELECT DISTINCT S2.artistID"
                                                    +"                                               FROM song S2"
                                                    +"                                               WHERE S2.genre = 'Pop')"
                                                    +" GROUP BY A.artistID"
                                                    +" HAVING avg(S.rating) > (SELECT "+"'"+rating+"'"+"+ 0.0) ORDER BY A.artistID");
                    

                    
            ResultSet resultset2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                    +" (SELECT DISTINCT A.artistID,A.artistName"
                                                    +" FROM artist A,song S"
                                                    +" WHERE A.artistID = S.artistID AND A.artistID IN"
                                                    +"                                               (SELECT DISTINCT S2.artistID"
                                                    +"                                               FROM song S2"
                                                    +"                                               WHERE S2.genre = 'Pop')"
                                                    +" GROUP BY A.artistID"
                                                    +" HAVING avg(S.rating) > (SELECT "+"'"+rating+"'"+"+ 0.0)) as S");

            resultset2.next();
            count = resultset2.getInt(1);
            resultset.next();
            Artist[] resultarray = new Artist[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new Artist(resultset.getInt(1),resultset.getString(2));
                resultset.next();
            }
            return resultarray;
        } catch (SQLException ex) {
           Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public Song[] retrieveLowestRatedAndLeastNumberOfListenedSongs() {
        try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            ResultSet resultset = stmt1.executeQuery(" SELECT S.songID,S.songName,S.genre,S.rating,S.artistID,S.albumID,sum(L.listenCount) as listencount"
                                                    +" FROM song S,listen L"
                                                    +" WHERE S.songID = L.songID AND S.genre = 'Pop' "
                                                    +" AND S.rating  = (SELECT min(S2.rating)"
                                                    +"                  FROM song S2"
                                                    +"                  WHERE S2.genre = 'Pop')"
                                                    +" GROUP BY S.songID"
                                                    +" HAVING listencount = (SELECT MIN(X.listencount2) FROM (SELECT sum(L2.listenCount) as listencount2"
                                                    +"                                                       FROM song S2,listen L2"
                                                    +"                                                       WHERE S2.songID = L2.songID AND S2.genre = 'Pop'"
                                                    +"                                                       GROUP BY S2.songID) as X) ORDER BY S.songID");
            
            ResultSet resultset2 = stmt2.executeQuery("SELECT COUNT(*) FROM"
                                                    +" (SELECT S.songID,S.songName,S.genre,S.rating,S.artistID,S.albumID,sum(L.listenCount) listencount"
                                                    +" FROM song S,listen L"
                                                    +" WHERE S.songID = L.songID AND S.genre = 'Pop' "
                                                    +" AND S.rating  = (SELECT min(S2.rating)"
                                                    +"                  FROM song S2"
                                                    +"                  WHERE S2.genre = 'Pop')"
                                                    +" GROUP BY S.songID"
                                                    +" HAVING listencount = (SELECT MIN(X.listencount2) FROM (SELECT sum(L2.listenCount) AS listencount2"
                                                    +"                                                       FROM song S2,listen L2"
                                                    +"                                                       WHERE S2.songID = L2.songID AND S2.genre = 'Pop'"
                                                    +"                                                       GROUP BY S2.songID) as X)) as S");

            resultset2.next();
            count = resultset2.getInt(1);
            resultset.next();
            Song[] resultarray = new Song[count];
            for(int i = 0;i<count;i++){
                resultarray[i] = new Song(resultset.getInt(1),resultset.getString(2),resultset.getString(3),resultset.getDouble(4),resultset.getInt(5),resultset.getInt(6));
                resultset.next();
            }
            return resultarray;
        } catch (SQLException ex) {
           Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public int multiplyRatingOfAlbum(String releaseDate) {
        try {
            int count = 0;
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();

            stmt1.executeUpdate("UPDATE album"
                              +" SET albumRating = albumRating * 1.5"
                              +" WHERE releaseDate > "+"'"+releaseDate+"'");
            
            ResultSet resultSet = stmt2.executeQuery("SELECT ROW_COUNT()");                                           
            resultSet.next();
            count = resultSet.getInt(1);
            return count;
        } catch (SQLException ex) {
            Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    @Override
    public Song deleteSong(String songName) {
        try {
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            ResultSet resultset = stmt1.executeQuery(" SELECT *"
                                                    +" FROM song S"
                                                    +" WHERE songName = "+"'"+songName+"'");
                                  stmt2.executeUpdate("DELETE FROM song"
                                                    +" WHERE songName = "+"'"+songName+"'");
            resultset.next();
            Song deletedsong = new Song(resultset.getInt(1),resultset.getString(2),resultset.getString(3),resultset.getDouble(4),resultset.getInt(5),resultset.getInt(6));
            return deletedsong;
        } catch (SQLException ex) {
           Logger.getLogger(MUSICDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
