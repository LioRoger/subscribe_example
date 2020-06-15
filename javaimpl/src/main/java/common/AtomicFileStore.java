package common;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

import static common.Util.*;

public class AtomicFileStore {
    private final String fileName;

    public AtomicFileStore(String fileName) {
        this.fileName = fileName;
    }

    public List<String> getContent() {
        List<String> ret = new LinkedList<>();
        if (!checkFileExists(fileName)) {
            return ret;
        }
        FileReader readFile = null;
        BufferedReader bufferedReader = null;
        try {
            readFile = new FileReader(fileName);
            bufferedReader = new BufferedReader(readFile);
            String s = null;
            while ((s = bufferedReader.readLine()) != null) {
                ret.add(s);
            }
        } catch (Exception e) {
        } finally {
            swallowErrorClose(readFile);
            swallowErrorClose(bufferedReader);
        }
        return ret;

    }

    public boolean updateContent(List<String> newContent)  {
        synchronized (this) {
            String tmpFileName = fileName + ".tmp";
            if (checkFileExists(tmpFileName)) {
                deleteFile(tmpFileName);
            }
            boolean writeSuccess = true;
            FileWriter fileWriter = null;
            BufferedWriter bufferedWriter = null;
            try {
                fileWriter = new FileWriter(tmpFileName);
                bufferedWriter = new BufferedWriter(fileWriter);
                for (String content : newContent) {
                    bufferedWriter.write(content);
                    bufferedWriter.newLine();
                }
                bufferedWriter.flush();
            } catch (Exception e) {
                writeSuccess = false;
            } finally {
                swallowErrorClose(fileWriter);
                swallowErrorClose(bufferedWriter);
            }
            // BugFix: windows can't rename file to existing file, remove old file first
            remove();
            return writeSuccess ? (new File(tmpFileName).renameTo(new File(fileName))) : false;
        }
    }

    public void remove()  {
        deleteFile(fileName);
    }

}
