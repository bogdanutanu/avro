package com.lordbogdan;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by bog on 21/03/17.
 */
public class MainWithSchema {

    private static List<Album> albums() {
        Album whatsTheStory = Album.newBuilder()
                .setArtist("Oasis")
                .setTitle("What's The Story (Morning Glory) ?")
                .setYear(1995)
                .build();

        Album appetiteForDestruction = Album.newBuilder()
                .setArtist("Guns 'n Roses")
                .setTitle("Appetite For Destruction")
                .setYear(1987)
                .build();

        Album am = Album.newBuilder()
                .setArtist("Arctic Monkeys")
                .setTitle("AM")
                .setYear(2013)
                .build();

        return Lists.newArrayList(whatsTheStory, appetiteForDestruction, am);
    }

    private static void write(String fileName) throws IOException {
        DatumWriter<Album> albumDatumWriter = new SpecificDatumWriter<>(Album.class);
        DataFileWriter<Album> albumDataFileWriter = new DataFileWriter<>(albumDatumWriter);
        List<Album> albums = albums();
        albumDataFileWriter.create(albums.get(0).getSchema(), new File(fileName));
        for (Album a : albums) {
            albumDataFileWriter.append(a);
        }
        albumDataFileWriter.close();
    }

    private static void read(File fileName) throws IOException {
        DatumReader<Album> albumDatumReader = new SpecificDatumReader<>(Album.class);
        DataFileReader<Album> dataFileReader = new DataFileReader<Album>(fileName,
                albumDatumReader);
        Album album = null;
        while (dataFileReader.hasNext()) {
            // reuse album object so we don't create a new object every time
            album = dataFileReader.next(album);
            System.out.println(album);
        }
    }

    public static void main(String args[]) throws Exception {

        write("albums.avro");

        read(new File("albums.avro"));
    }
}
