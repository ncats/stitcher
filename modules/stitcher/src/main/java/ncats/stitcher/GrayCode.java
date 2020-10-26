package ncats.stitcher;

import java.util.function.Consumer;

public class GrayCode {
    private int maxsize = 0, size = 0;
    private int[] n, g, u, c;

    public GrayCode (int N, int k) {
        init (N, k);
    }

    public GrayCode (int[] N) {
        init (N);
    }

    protected void init (int N, int k) {
        n = new int[k+1];
        g = new int[k+1];
        u = new int[k+1];
        c = new int[k]; // copy of g

        for (int i = 0; i <= k; ++i) {
            g[i] = 0;
            u[i] = 1;
            n[i] = N;
        }
        size = 0;
    }

    protected void init (int[] N) {
        int k = N.length;
        n = new int[k+1];
        g = new int[k+1];
        u = new int[k+1];
        c = new int[k]; // copy of g

        int min = Integer.MAX_VALUE;
        for (int i = 0; i < k; ++i) {
            g[i] = 0;
            u[i] = 1;
            n[i] = N[i];
            if (N[i] < min) {
                min = N[i];
            }
        }
        g[k] = 0;
        u[k] = 1;
        n[k] = min;
        size = 0;
    }

    public void generate (Consumer<int[]> consumer) {
        if (consumer != null) {
            for(int i, j; g[c.length] == 0;) {
                System.arraycopy(g, 0, c, 0, c.length);
                consumer.accept(c);
        
                i = 0; 
                j = g[0] + u[0];
                while (((j >= n[i]) || (j < 0)) && (i < c.length)) {
                    u[i] = -u[i];
                    ++i;
                    j = g[i] + u[i];
                }
                g[i] = j;

                ++size;
                if (maxsize > 0 && size >= maxsize) {
                    break;
                }
            }
        }
    }

    public void setMaxSize (int maxsize) {
        this.maxsize = maxsize;
    }
    public int getMaxSize () { return maxsize; }

    public int size () { return size; }

    public static GrayCode createBinaryCode (int size) {
        return new GrayCode (2, size);
    }

    public static class Enum {
        public static void main (final String[] argv) throws Exception {
            // all possible subsets = 2^k
            if (argv.length == 0) {
                System.out.println("Usage: GrayCode$Enum k\n");
                System.exit(1);
            }

            GrayCode g = createBinaryCode (Integer.parseInt(argv[0]));
            g.generate(c -> {
                    for (int i = c.length; --i >= 0;) {
                        System.out.print(c[i]);
                    }
                    System.out.println();
                });
            System.out.println("## "+g.size()+" codes generated!");
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.out.println("Usage: GrayCode [n0 n1...]\n");
            System.out.println("generate all permutation codes "
                               +"from 0..n0-1, 0..n1-1, ...");
            System.exit(1);
        }

        int[] a = new int[argv.length];
        for (int i = 0; i < a.length; ++i) {
            a[i] = Integer.parseInt(argv[i]);
        }
        
        GrayCode g = new GrayCode (a);
        g.generate(c -> {
                for (int i = 0; i < c.length; ++i) {
                    System.out.print(" " + c[i]);
                }
                System.out.println();
            });
        System.out.println("## "+g.size()+" codes generated!");
    }
}
