import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileTest {
	public static void main(String[] args) {
		String message = "1234567890abcdefghijklmnopqrstuvwxyz";
		String path = "E://concurrent_file_write.txt";
		try {
			FileWriter writer = new FileWriter(new File(path));
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			for (long i = 0; i < 1000 * 1000 * 1000; i++) {
				bufferedWriter.write(message + "\n");
			}
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
			String temp = null;
			while ((temp = reader.readLine()) != null) {
				System.out.println(temp);
				break;
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class ConcWriter extends Thread {
		BufferedWriter writer;
		File file;

		public void run() {
			
		}
	}
}
