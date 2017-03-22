package com.lordbogdan;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * Created by bog on 21/03/17.
 */
public class MainWithOutSchema {

    /* Let's deserialize the objects created in the @see MainWithSchema class and stored in the
     * 'albums.avro' file. We'll see  */

    public static void main(String args[]) {

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    }
}
