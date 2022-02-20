import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Clasa Mapper care executa metoda de map.
 */
public class Mapper implements MapReduceInterface {
    public final String file_name;
    private final int D;
    private final int offset;
    private final ArrayList<String> allInformation = new ArrayList<>();
    public Pair<HashMap<Integer, Integer>, ArrayList<String>> result;

    /**
     *
     * @param file_name numele fisierului
     * @param D cati bytes ii revin task-ului
     * @param offset offset-ul de la care trebuie citit din fisier
     */
    public Mapper(String file_name, int D, int offset) {
        this.file_name = file_name;
        this.D = D;
        this.offset = offset;
    }

    /**
     * Metoda de map care ia toate cuvintele si creaza HashMap-ul cu
     * dimensiunile acestora precum si ArrayList-ul cu cele mai lungi
     * cuvinte.
     */
    public void map() {
        try {
            readInformation();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HashMap<Integer, Integer> map = new HashMap<>();
        ArrayList<String> longestWords = new ArrayList<>();

        int max = 0;
        for (var string : allInformation) {
            if (string.equals(""))
                continue;

            map.putIfAbsent(string.length(), 0);
            map.put(string.length(), map.get(string.length()) + 1);
            if (max < string.length()) {
                max = string.length();
                longestWords.clear();
                longestWords.add(string);
            } else if (max == string.length()) {
                longestWords.add(string);
            }
        }
        result = new Pair<>(map, longestWords);
    }

    /**
     * Metoda care citeste informatia din fisier.
     * Prima data trece peste posibilul cuvant incomplet de la inceputul
     * zonei care ii revine task-ului, apoi citeste informatia. Dupa aceea,
     * citeste si posibilul cuvant incomplet de la final si imparte
     * intreaga informatie cu ajutorul regex-ului "[^a-zA-Z0-9]".
     */
    private void readInformation() throws IOException {
        RandomAccessFile file = new RandomAccessFile(new File(file_name), "r");
        file.seek(offset);
        int displacement = 0;
        if (offset - 1 >= 0) {
            file.seek(offset-1);
            byte info = file.readByte();
            if (Character.isLetterOrDigit(info)) {
                while (Character.isLetterOrDigit(info)) {
                    info = file.readByte();
                    displacement++;
                    if (displacement >= D)
                        return;
                }
            }
        }
        ArrayList<Byte> information = new ArrayList<>();
        byte b;
        byte[] bytes = new byte[D - displacement];
        file.read(bytes, 0, D - displacement);
        b = bytes[D - displacement - 1];
        while (Character.isLetterOrDigit((char) b)) {
            try {
                b = file.readByte();
            } catch (EOFException e) {
                break;
            }
            information.add(b);
        }

        file.close();
        StringBuilder stringBuilder = new StringBuilder();
        for (var oneByte : bytes)
            stringBuilder.append((char) oneByte);
        for (var c : information)
            stringBuilder.append((char) c.byteValue());
        stringBuilder.append(' ');
        String[] tokens = stringBuilder.toString().split("[^a-zA-Z0-9]");
        allInformation.addAll(Arrays.asList(tokens));
    }
}
