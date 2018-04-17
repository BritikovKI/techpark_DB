package com.hw.db.DAO;

import com.hw.db.models.*;
import com.hw.db.models.Thread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.hw.db.DAO.PostDAO.getPost;


@Service
@Transactional
public class ThreadDAO {
    private static JdbcTemplate jdbc;
    private static IntegerMapper INT_MAPPER=new IntegerMapper();
    private static ThreadDAO.ThreadMapper THREAD_MAPPER = new ThreadDAO.ThreadMapper();
    private static PostDAO.PostMapper POST_MAPPER = new PostDAO.PostMapper();

    @Autowired
    public ThreadDAO(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static Thread getThreadById(Integer id){
        String SQL="SELECT * FROM \"threads\" WHERE id=? LIMIT 1;";
        return jdbc.queryForObject(SQL,THREAD_MAPPER,id);
    }
    public static Thread getThreadBySlug(String slug){
//        Number i=Integer.parseInt(slug);
        String SQL="SELECT * FROM \"threads\" WHERE slug::CITEXT = (?)::CITEXT LIMIT 1;";
//        String SQL=" SELECT id FROM \"threads\" WHERE id=1 LIMIT 1;";
        Thread res=jdbc.queryForObject(SQL,THREAD_MAPPER,slug);
        return res;
    }


    public static Integer change(Vote vote, Integer res){
        Integer def=jdbc.queryForObject("SELECT voice FROM \"votes\" WHERE tid=? AND nickname=?;"
                ,Integer.class,vote.getTid(),vote.getNickname());
        String SQL="UPDATE \"votes\" SET voice=? WHERE tid=? AND nickname =?;";
        jdbc.update(SQL,vote.getVoice(),vote.getTid(),vote.getNickname());
        SQL="UPDATE \"threads\" SET votes=votes-?+? WHERE id=?; ";
        jdbc.update(SQL,def,vote.getVoice(),vote.getTid());
        return (res-def+vote.getVoice());
    }

    public static void createVote(Thread th, Vote vote) {
        String SQL="INSERT INTO \"votes\" (nickname, voice,tid) VALUES (?,?,?)";
        jdbc.update(SQL,vote.getNickname(),vote.getVoice(),vote.getTid());
        SQL="UPDATE \"threads\" SET votes=votes+? WHERE id=?;";
        jdbc.update(SQL,vote.getVoice(),th.getId());
    }

    public static void change(Thread before,Thread th){
        List<Object> lst=new ArrayList<>();
        Boolean flag=false;
        String SQL="UPDATE \"threads\" SET";

        if(th.getCreated()!=null)
        {
            SQL+=" created=?::TIMESTAMPTZ ";
            flag=true;
            lst.add(th.getCreated());
        }

        if(th.getSlug()!=null)
        {
            if(flag)
            {
                SQL+=",";
            }
            SQL+=" slug=?::CITEXT ";
            lst.add(th.getSlug());
            flag=true;
        }

        if(th.getAuthor()!=null)
        {
            if(flag)
            {
                SQL+=",";
            }
            SQL+="author=?::CITEXT";
            lst.add(th.getAuthor());
            flag=true;
        }
        if(th.getMessage()!=null)
        {
            if(flag)
            {
                SQL+=",";
            }
            SQL+=" message=? ";
            lst.add(th.getMessage());
            flag=true;
        }
        if(th.getTitle()!=null)
        {
            if(flag)
            {
                SQL+=",";
            }
            SQL+=" title=? ";
            lst.add(th.getTitle());
            flag=true;
        }
        if(flag)
        {
//            SQL+=", isEdited=TRUE ";
            SQL+=" WHERE id=?; ";
            lst.add(before.getId());

            jdbc.update(SQL,lst.toArray());
        }

    }

    public static List<Post> getPosts(Integer id, Integer limit, Integer since, String sort, Boolean desc) {
        String SQL="SELECT * FROM \"posts\" WHERE thread=? ";
        List<Object> lst= new LinkedList<>();
        lst.add(id);


        if (sort!=null) {
            if(sort.equals("flat")) {
                if (since!=null)
                {
                    if (desc != null && desc == true )
                    {
                        SQL+= " AND id<? ";
                    }
                    else {
                        SQL += " AND id>? ";
                    }
                    lst.add(since);
                }
                SQL += " ORDER BY ";
                SQL += " created ";
                if (desc != null && desc.equals(true))
                {
                    SQL+=" DESC ";
                }
                SQL += " ,id   ";
            }

            if(sort.equals("tree")){
                if (since != null) {
                    if (desc != null && desc.equals(true)) {
                        SQL += " AND branch < (SELECT branch FROM posts WHERE id = ?) ";

                    }
                    else{
                        SQL += " AND branch > (SELECT branch FROM posts WHERE id = ?) ";
                    }
                    lst.add(since);
                }
                SQL += " ORDER BY branch";
            }

            if(sort.equals("parent_tree")){
                SQL="SELECT * FROM  posts WHERE ";
                lst.clear();
                lst.add(id);
                if(limit!=null){
                    SQL+=" branch[1] IN (SELECT DISTINCT branch[1] FROM" +
                            " posts WHERE thread=? ";
                    if(since != null)
                    {
                        if (desc != null && desc.equals(true)) {
                            SQL += " AND branch[1] < (SELECT branch[1] FROM posts WHERE id = ?) ";

                        }
                        else{
                            SQL += " AND branch[1] > (SELECT branch[1] FROM posts WHERE id = ?) ";
                        }
                        lst.add(since);
                    }

                    SQL+=" ORDER BY branch[1] ";
                    if (desc != null && desc.equals(true)) {
                        SQL += " DESC ";

                    }
                    SQL+=" LIMIT ?) ";

                    lst.add(limit);
                }
                else {
                    SQL+=" thread=? ;";
                }
                SQL += " ORDER BY branch[1] ";
                if (desc != null && desc.equals(true))
                {
                    SQL+=" DESC ";
                }


                SQL+=",branch,id";
                SQL+=";";
                return jdbc.query(SQL,POST_MAPPER,lst.toArray());
            }
        }
        if(sort==null)
        {
            if (since!=null)
            {
                if (desc != null && desc == true )
                {
                    SQL+= " AND id<? ";
                }
                else {
                    SQL += " AND id>? ";
                }
                lst.add(since);
            }
            SQL+=" ORDER BY ID";
        }


        if (desc != null && desc == true )
        {
            SQL+=" DESC ";
        }

        if(limit!=null)
        {
            SQL+=" LIMIT ? ";
            lst.add(limit);
        }

        SQL+=";";
        return jdbc.query(SQL,POST_MAPPER,lst.toArray());
    }


    public static final class IntegerMapper implements RowMapper<Integer> {
        public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getInt("id");
        }
    }

    public static void createPosts(String slug,List<Post> posts) {
        Timestamp curr=new Timestamp(Instant.now().toEpochMilli());
        String SQL = " INSERT INTO \"posts\" (message, created, author,forum, thread,parent) VALUES (?,(?)::TIMESTAMP WITH TIME ZONE ,(?)::CITEXT,?,?,?) RETURNING id; ";
        for (Post post: posts
                ) {
                if(post.getParent()!=null){
                   if(!getPost(post.getParent()).getThread().equals(post.getThread()))
                   {
                       throw new DuplicateKeyException("Parent is in another castle.");
                   }
                }
                if(post.getCreated()==null)
                {
                    post.setCreated(curr);
                }
                post.setId( jdbc.queryForObject(SQL, Integer.class, post.getMessage(), post.getCreated(), post.getAuthor(), post.getForum(), post.getThread(), post.getParent()));
                SetTree(post);
                jdbc.update("UPDATE \"forums\" SET posts=posts+1 WHERE slug=?",post.getForum());
        }


    }

    private static void SetTree(Post post) {
        jdbc.update(connection -> {
            PreparedStatement pst=connection.prepareStatement("UPDATE \"posts\" SET branch=? WHERE id=?;", PreparedStatement.RETURN_GENERATED_KEYS);
            if(post.getParent()==null) {
                pst.setArray(1, connection.createArrayOf("INT", new Object[]{post.getId()}));
            } else {
                Post par= getPost(post.getParent());
                ArrayList arr = new ArrayList<Object>(Arrays.asList(par.getBranch()));
                arr.add(post.getId());
                pst.setArray(1, connection.createArrayOf("INT", arr.toArray()));
            }
            pst.setLong(2, post.getId());
            return pst;
        });

    }

    public static final class ThreadMapper implements RowMapper<Thread> {
        public Thread mapRow(ResultSet rs, int rowNum) throws SQLException {
            final Thread th = new Thread();
            th.setId(rs.getInt("id"));
            th.setTitle(rs.getString("title"));
            th.setSlug(rs.getString("slug"));
            th.setCreated(rs.getTimestamp("created"));
            th.setMessage(rs.getString("message"));
            th.setAuthor(rs.getString("author"));
            th.setForum(rs.getString("forum"));
            th.setVotes(rs.getInt("votes"));
            return th;
        }
    }

}
