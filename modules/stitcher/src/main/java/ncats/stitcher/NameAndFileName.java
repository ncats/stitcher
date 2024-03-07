package ncats.stitcher;

public class NameAndFileName {
    public String Name;
    public String FileName;

    public NameAndFileName(String name, String fileName) {
        this.Name = name;
        this.FileName = fileName;
    }

    @Override
    public String toString() {
        return this.Name + " : " + this.FileName;
    }
}
