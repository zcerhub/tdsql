package com.tencent;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TdsqlTest {


    public static void main(String[] args) throws Exception {
        TdsqlTest test = new TdsqlTest();
        String filePath="data/src_a/a/test2.csv";
        File file=new File(filePath);
        test.readFileBySplit(filePath);
//        test.readFileByBlock(filePath);
//        test.readFileByBuffer(filePath);
//        System.out.println(file.length()/81);
//        System.out.println(file.length()%81);
//        System.out.println("1867769670,32.2880798773275,d6627115d2dceb272bea4487c1cc1541,2021-10-18 13:02:10\n1867769670,32.2880798773275,d6627115d2dceb272bea4487c1cc1541,2021-10-18 13:02:10".toCharArray().length);
/*        for (int i = 0; i < 10; i++) {
            long startTime=System.currentTimeMillis();
            test.readFileByBlock(filePath);
//            test.readFileByBuffer(filePath);
            long endTime = System.currentTimeMillis();
            System.out.println((endTime - startTime));
        }*/
    }




    private void readFileByBuffer(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line=null;
        int count=0;
        while ((line = br.readLine()) != null) {
//            System.out.println(line);
            count++;
        }
        System.out.println(count);
    }

    private void readFileByBlock(String filePath) throws IOException {
       FileInputStream fis = new FileInputStream(filePath);
        byte[] buf = new byte[1024 * 64];
        int bufLen=0;
        StringBuilder sb = new StringBuilder();
        int count=0;
        while ((bufLen=fis.read(buf)) != -1) {
            for(int i=0;i<bufLen;i++) {
                if (buf[i] != '\n') {
                    sb.append((char)buf[i]);
                }else{
                    count++;
//                    System.out.println(sb.toString());
                    System.out.println(sb.length());
                    sb=new StringBuilder();
                }
            }
        }
        System.out.println(count);
        if (sb.length() > 0) {
//            System.out.println(sb.toString());
        }
    }


    private void readFileBySplit(String filePath) throws  Exception {
        File file=new File(filePath);
        System.out.println(file.length());
        long fileSize=file.length();
        int partition = 4;
        int blockSize= (int) (fileSize/partition);
        CountDownLatch cd=new CountDownLatch(partition);
        FileInputStream fis = new FileInputStream(file);

        long[] startPosPartitaion=new long[partition];
        startPosPartitaion[0]=0;
        RandomAccessFile raf = new RandomAccessFile(filePath,"r");
        int maxBytesPerLines=100;
        for (int i = 1; i < partition; i++) {
            long currentPartStartPos=fileSize/partition*i;
            MappedByteBuffer mbb=raf.getChannel().map(FileChannel.MapMode.READ_ONLY,currentPartStartPos,maxBytesPerLines);
            for (int j = 0; j < maxBytesPerLines; j++) {
                byte bt=mbb.get();
                if (bt == (byte)'\n') {
                    currentPartStartPos+=j+1;
                    break;
                }
            }
            startPosPartitaion[i]=currentPartStartPos;
        }

        FileChannel fileChannel=raf.getChannel();
        for (int i = 0; i < partition; i++) {
            int j=i;
            new Thread(()->{
                long readPos=startPosPartitaion[j],endPos=0;
                if(j==partition-1){
                    endPos=fileSize;
                }else{
                    endPos = startPosPartitaion[j + 1];
                }
                int readBufferSize=1024;
                ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferSize);
                byte[] readBytes =null;
                try {
                    while (readPos < endPos) {
                        byteBuffer.clear();
                        fileChannel.read(byteBuffer, readPos);
                        readBytes=byteBuffer.array();
                        int hasReadPos=0,lastLine=0;
                        List<List<String>> records = new ArrayList<>();
                        List<String> record=new ArrayList<>();
                        StringBuilder sb = new StringBuilder();
                        for (; hasReadPos < endPos; hasReadPos++) {
                            if (readBytes[hasReadPos] == '\n') {
                                lastLine = hasReadPos;
                                record.add(sb.toString());
                                records.add(record);
                                record=new ArrayList<>();
                            }else{
                                if (readBytes[hasReadPos] == ',') {
                                    record.add(sb.toString());
                                    sb = new StringBuilder();
                                }else{
                                    sb.append((char)readBytes[hasReadPos]);
                                }
                            }
                        }
                        System.out.println(records);
                        readPos+=lastLine+1;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    cd.countDown();
                }
            }).start();
        }
        cd.await();
    }


//    private void readFileBySplit(String filePath) throws  Exception {
//        File file=new File(filePath);
//        System.out.println(file.length());
//        long fileSize=file.length();
//        int partition = 4;
//        int blockSize= (int) (fileSize/partition);
//        CountDownLatch cd=new CountDownLatch(partition);
//        FileInputStream fis = new FileInputStream(file);
//
//        long[] startPosPartitaion=new long[partition];
//        startPosPartitaion[0]=0;
//        RandomAccessFile raf = new RandomAccessFile(filePath,"r");
//        int maxBytesPerLines=100;
//        for (int i = 1; i < partition; i++) {
//            long currentPartStartPos=fileSize/partition*i;
//            MappedByteBuffer mbb=raf.getChannel().map(FileChannel.MapMode.READ_ONLY,currentPartStartPos,maxBytesPerLines);
//            for (int j = 0; j < maxBytesPerLines; j++) {
//                byte bt=mbb.get();
//                if (bt == (byte)'\n') {
//                    currentPartStartPos+=j+1;
//                    break;
//                }
//            }
//            startPosPartitaion[i]=currentPartStartPos;
//        }
//
//        FileChannel fileChannel=raf.getChannel();
//        for (int i = 0; i < partition; i++) {
//            int j=i;
//            new Thread(()->{
//                long readPos=startPosPartitaion[j],endPos=0;
//                if(j==partition-1){
//                    endPos=fileSize;
//                }else{
//                    endPos = startPosPartitaion[j + 1];
//                }
//                int readBufferSize=32;
//                ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferSize);
//                byte[] readBytes =null;
//                try {
//                    while (readPos < endPos - 1) {
//                        int size = (int) Math.min(readBufferSize, endPos - readPos);
//                        byteBuffer.clear();
//                        fileChannel.read(byteBuffer, readPos);
//                        readBytes = byteBuffer.array();
//                        while (size > 0) {
//                            if (readBytes[size-1] == '\n') {
//                                break;
//                            }
//                            size--;
//                        }
//                        List<List<String>> records = new ArrayList<>();
//                        for (int k = 0; k < size;) {
//                            int n=k;
//                            List<String> record=new ArrayList<>();
//                            StringBuilder sb = new StringBuilder();
//                            while (n < size && readBytes[n] != '\n') {
//                                int m=n;
//                                while (m < size && readBytes[m] != ',') {
//                                    sb.append(readBytes[m++]);
//                                }
//                                record.add(sb.toString());
//                                sb=new StringBuilder();
//                                n=m+1;
//                            }
//                            System.out.println(record);
//                            records.add(record);
//                            k=n+1;
//                        }
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }finally {
//                    cd.countDown();
//                }
//            }).start();
//        }
//        cd.await();
//    }



    private void processRecord(List<String> recordsList) {
        for (String field : recordsList) {
            System.out.print(field+"   ");
        }
    }


}
