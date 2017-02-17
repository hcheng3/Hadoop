/**
 * Created by jingjunzhang on 2/9/17.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateDataset {

        public static void main(String[] args) {
            int numCustomer = 50000;
            int numTransaction = 5000000;

            String alphabet = "abcdefghijklmnopqrstuvwxyz ";

            String filePathCustomer = "/Users/jingjunzhang/Downloads/customer.csv";
            String filePathTransaction = "/Users/jingjunzhang/Downloads/transaction.csv";

            File fileC = new File(filePathCustomer);
            File fileT = new File(filePathTransaction);

            try {
                FileWriter fwc = new FileWriter(fileC, true);
                FileWriter fwt = new FileWriter(fileT, true);

                BufferedWriter bwc = new BufferedWriter(fwc);
                BufferedWriter bwt = new BufferedWriter(fwt);

                for(int i = 0; i < numCustomer; i++){
                    StringBuilder tempStr = new StringBuilder();
                    tempStr.append(Integer.toString(i + 1) + ",");
                    int rdm = 10 + (int)Math.floor(Math.random() * 11);
                    for(int j = 0; j < rdm; j++){
                        tempStr.append(alphabet.charAt((int)Math.floor(Math.random() * 26)));
                    }
                    tempStr.append(",");
                    tempStr.append(Integer.toString(10 + (int)Math.floor(Math.random() * 61)) + ",");
                    tempStr.append(Integer.toString((int)Math.ceil(Math.random() * 10)) + ",");
                    tempStr.append(Double.toString((1000 + (int)Math.floor(Math.random() * 99010)) / 10.0) + "\n");
                    bwc.write(tempStr.toString());
                }
                for(int i = 0; i < numTransaction; i++){
                    StringBuilder tempStr = new StringBuilder();
                    tempStr.append(Integer.toString(i + 1) + ",");
                    tempStr.append(Integer.toString((int)Math.ceil(Math.random() * numCustomer)) + ",");
                    tempStr.append(Double.toString((100 + (int)Math.floor(Math.random() * 9910)) / 10.0) + ",");
                    tempStr.append(Integer.toString((int)Math.ceil(Math.random() * 10)) + ",");

                    int rdm = 20 + (int)Math.floor(Math.random() * 31);
                    for(int j = 0; j < rdm; j++){
                        tempStr.append(alphabet.charAt((int)Math.floor(Math.random() * 26)));
                    }
                    tempStr.append("\n");
                    bwt.write(tempStr.toString());
                }

                bwc.flush();
                bwc.close();
                fwc.close();
                bwt.flush();
                bwt.close();
                fwt.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
}
