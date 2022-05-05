import java.io.Serializable;
public class Record implements Serializable {
    public int user_id;
    public int user_age;
    public byte satisfaction_score;

    public Record(int user_id, int user_age, byte satisfaction_score)
    {
        this.user_id = user_id;
        this.user_age = user_age;
        this.satisfaction_score = satisfaction_score;
    }
    @Override
    public String toString() {
        return "Record [user_id=" + user_id + ", user_age=" + user_age+", satisfaction_score="+satisfaction_score
                + "]";
    }
  }
