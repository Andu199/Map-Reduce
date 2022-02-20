import java.util.ArrayList;
import java.util.HashMap;

/**
 * Clasa Reducer care executa metoda de reduce.
 */
public class Reducer implements MapReduceInterface {
    private String file_name;
    private final ArrayList<Pair<HashMap<Integer, Integer>, ArrayList<String>>> input;
    public String result = "";
    public float rank = 0;

    /**
     *
     * @param file_name numele fisierului
     * @param input lista de pair-uri de hashmap si lista de longest words
     *              (practic, lista de rezultate ale Mapper-urilor).
     */
    public Reducer(String file_name,
        ArrayList<Pair<HashMap<Integer, Integer>, ArrayList<String>>> input) {
        this.file_name = file_name;
        this.input = input;
    }

    private int fib(Integer n) {
        int a = 0, b = 1, c;
        if (n == 0)
            return a;
        if (n == 1)
            return b;
        for (int i = 2; i <= n; i++)
        {
            c = a + b;
            a = b;
            b = c;
        }
        return b;
    }

    /**
     * Combina toate HashMap-urile si toate ArrayList-urile, apoi calculeaza
     * rank-ul fisierului dupa formula descrisa in enunt.
     * La final, creaza string-ul care trebuie afisat.
     */
    public void reduce() {
        HashMap<Integer, Integer> map = new HashMap<>();
        ArrayList<String> longestWords = new ArrayList<>();
        int maxLen = 0;
        for (var pair : input) {
            for (var entry : pair.getFirst().entrySet()) {
                map.putIfAbsent(entry.getKey(), 0);
                map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
            }
            if (pair.getSecond().size() == 0)
                continue;
            if (maxLen == pair.getSecond().get(0).length()) {
                longestWords.addAll(pair.getSecond());
            } else if (maxLen < pair.getSecond().get(0).length()) {
                longestWords.clear();
                longestWords.addAll(pair.getSecond());
                maxLen = longestWords.get(0).length();
            }
        }
        int totalNo = 0;
        for (var entry : map.entrySet()) {
            rank += (fib(entry.getKey() + 1) * entry.getValue());
            totalNo += entry.getValue();
        }
        rank /= totalNo;
        String[] tokens = file_name.split("/");
        file_name = tokens[tokens.length - 1];
        result = file_name + "," + String.format("%.2f", rank) + "," + maxLen + "," + longestWords.size();
    }
}
