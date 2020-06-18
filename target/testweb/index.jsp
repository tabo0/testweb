<%--
  Created by IntelliJ IDEA.
  User: ASUS
  Date: 2020/6/15
  Time: 22:15
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="s" uri="/struts-tags"%>
<html>
<head>
    <title>Title</title>
    <link href="new.css" rel="stylesheet" type="text/css"/>
    <link rel="icon" href="myico.ico" type="image/x-icon" />
    <script src="http://code.jquery.com/jquery-1.4.1.min.js"></script>
    <link rel="stylesheet" href="https://cdn.staticfile.org/twitter-bootstrap/4.1.0/css/bootstrap.min.css">
    <script src="https://cdn.staticfile.org/jquery/3.2.1/jquery.min.js"></script>
    <script src="https://cdn.staticfile.org/popper.js/1.12.5/umd/popper.min.js"></script>
    <script src="https://cdn.staticfile.org/twitter-bootstrap/4.1.0/js/bootstrap.min.js"></script>

    <link rel="stylesheet" href="https://cdn.staticfile.org/twitter-bootstrap/3.3.7/css/bootstrap.min.css">
    <script src="https://cdn.staticfile.org/twitter-bootstrap/3.3.7/js/bootstrap.min.js"></script>
</head>
<body>
<div class="container mar ">
    <h1 class="newcenter" align="center"> 微博用户数据查询显示<span style="color: red">*必填</span></h1>

    <div>
    <form class="form-horizontal" role="form">
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" width="800" id="name" placeholder="查询一个用户关注了谁？">

            </div>

        </div>
    </form>
        <div class="form-group">
            <div class="col-sm-offset-2 col-sm-10">
                <button type="submit" class="btn btn-default" onclick="return submit()">提交</button>
            </div>
        </div>
    </div>

    <form class="form-horizontal" role="form">
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户A id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="idA" placeholder="用户 A 关注了用户 B 吗?">
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户B id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="idB" placeholder="用户 A 关注了用户 B 吗?">
            </div>
        </div>
    </form>
    <div class="form-group">
        <div class="col-sm-offset-2 col-sm-10">
            <button type="submit" class="btn btn-default" onclick="return AisfollowB()">提交</button>
        </div>
    </div>



    <form class="form-horizontal" role="form">
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户A id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="id" placeholder="谁关注了用户 A？">
            </div>
        </div>
    </form>
    <div class="form-group">
        <div class="col-sm-offset-2 col-sm-10">
            <button type="submit" class="btn btn-default" onclick="return fans()">提交</button>
        </div>
    </div>

    <form class="form-horizontal" role="form">
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户A id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="writeA" placeholder="用户 A 关注用户 B">
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户B id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="writeB" placeholder="用户 A 关注用户 B">
            </div>
        </div>
    </form>
    <div class="form-group">
        <div class="col-sm-offset-2 col-sm-10">
            <button type="submit" class="btn btn-default" onclick="return Write()">提交</button>
        </div>
    </div>

    <form class="form-horizontal" role="form">
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户A id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="deleteA" placeholder="用户 A 取消关注用户 B">
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-2 control-label">用户B id<span style="color: red"> *</span></label>
            <div class="col-sm-10">
                <input type="text" class="form-control" id="deleteB" placeholder="用户 A 取消关注用户 B">
            </div>
        </div>
    </form>
    <div class="form-group">
        <div class="col-sm-offset-2 col-sm-10">
            <button type="submit" class="btn btn-default" onclick="return Delete()">提交</button>
        </div>
    </div>
</div>
<div id="content">

</div>
<script>
    function  submit(){
        var name =$('#name').val();
        if(name.length ==0 ||name.length>30){
            alert('输入长度不符合!');
            return false;
        }
        var divshow = $("#content");
        divshow.text("正在查询中，请耐心等待...");
        var con = {
            id:name,flag:'findFollow',
        };
        var path="http://localhost:8080/"
        path="http://119.3.167.84:8080/testweb/"
        $.post(path+'findfollow',con,function(data){
            var divshow = $("#content");
            divshow.text("");// 清空数据
            divshow.append(data);
        });
        //window.location.href = "job_title.php";
    }

    function  AisfollowB(){
        var idA =$('#idA').val();
        var idB =$('#idB').val();
        if(idA.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        if(idB.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        var divshow = $("#content");
        divshow.text("正在查询中，请耐心等待...");
        var con = {
            idA:idA,idB:idB,flag:"AIsfollowB",
        };
        var path="http://localhost:8080/"
        path="http://119.3.167.84:8080/testweb/"
        $.post(path+'AIsfollowB',con,function(data){
            var divshow = $("#content");
            divshow.text("");// 清空数据
            divshow.append(data);
        });
        //window.location.href = "job_title.php";
    }

    function  fans(){
        var id =$('#id').val();
        if(id.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        var divshow = $("#content");
        divshow.text("正在查询中，请耐心等待...");
        var con = {
            id:id,flag:"fans",
        };
        var path="http://localhost:8080/"
        path="http://119.3.167.84:8080/testweb/"
        $.post(path+'fans',con,function(data){
            var divshow = $("#content");
            divshow.text("");// 清空数据
            divshow.append(data);
        });
        //window.location.href = "job_title.php";
    }

    function  Write(){
        var idA =$('#writeA').val();
        var idB =$('#writeB').val();
        if(idA.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        if(idB.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        var divshow = $("#content");
        divshow.text("正在写入中，请耐心等待...");
        var con = {
            idA:idA,idB:idB,flag:"write",
        };
        var path="http://localhost:8080/"
        path="http://119.3.167.84:8080/testweb/"
        $.post(path+'write',con,function(data){
            var divshow = $("#content");
            divshow.text("关注成功！");// 清空数据
        });
        //window.location.href = "job_title.php";
    }
    function Delete(){
        var idA =$('#deleteA').val();
        var idB =$('#deleteB').val();
        if(idA.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        if(idB.length ==0 ){
            alert('输入长度不符合!');
            return false;
        }
        var divshow = $("#content");
        divshow.text("正在修改中，请耐心等待...");
        var con = {
            idA:idA,idB:idB,flag:"delete",
        };
        var path="http://localhost:8080/"
        path="http://119.3.167.84:8080/testweb/"
        $.post(path+'delete',con,function(data){
            var divshow = $("#content");
            divshow.text("取消关注成功！");// 清空数据
        });
        //window.location.href = "job_title.php";
    }
</script>
</body>
</html>
