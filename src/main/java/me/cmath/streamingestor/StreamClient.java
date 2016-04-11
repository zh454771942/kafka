package me.cmath.streamingestor;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashSet;
import me.cmath.streamingestor.MainIngestor;
import org.junit.After;
import org.junit.Test;


public class StreamClient {

  private final static String IP = "localhost";
  private final static int PORT = 18000;


      public static void run() {
      Socket clientSocket = null;
      try {

        clientSocket = new Socket(IP, PORT);
        clientSocket.setSoTimeout(5000);

        BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
        String messageString = "";
        int totalTuples = 0;

        long start = System.currentTimeMillis();
        HelloKafkaProducer kProduce = new HelloKafkaProducer();
        while (true) {
          int length = in.read();
          if(length == -1 || length == 0) {
            break;
          }
          byte[] messageByte = new byte[length];
          in.read(messageByte);
          totalTuples++;
          messageString = new String(messageByte);
          System.out.println(messageString);
          
          // connect to kafka
          kProduce.getMessage(messageString);
          
        }
        clientSocket.close();
        long end = System.currentTimeMillis();

        double duration = (end - start);
        double tuplesPerSec = totalTuples / (duration / 1000);

        System.out.println("** Emitted " + totalTuples + " tuples in " + duration + " ms");
        System.out.println("** Txn rate: " + tuplesPerSec + " tuples/sec");
      } catch (InterruptedIOException e) {
        System.out.println("Timed out!");
//        fail();
      } catch (UnknownHostException e) {
        System.out.println("Sock:" + e.getMessage());
//        fail();
      } catch (EOFException e) {
        System.out.println("EOF:" + e.getMessage());
//        fail();
      } catch (IOException e) {
        System.out.println("IO:" + e.getMessage());
//        fail();
      }
    }
    public static void main(String[] args) {
      run();
    }
}