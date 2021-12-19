package com.tencent;

import java.io.*;

public class FileTest {

    public static void main(String[] args) throws IOException {
        String filePath = "data/src_a/a/test1.csv";
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = "";
        long pos=0;
        while ((line = br.readLine())!=null) {
            System.out.println(line+"       "+pos);
            pos+=line.length()+1;
        }

    }


}
