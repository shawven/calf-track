<script src="../../../../../../html/vue-element-admin/src/store/index.js"></script>
<template>
  <el-container>
    <el-header style="display: flex; justify-content: space-between; background-color:#545c64">
      <div>
        <h2 class="header" style="color:#fff">track-web</h2>
      </div>
      <el-select v-model="namespace" placeholder="请选择" @change="reload">
        <el-option
          v-for="item in namespaces"
          :key="item"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    <!-- </el-header>
    <el-header> -->
      <el-dropdown trigger="hover">
        <span class="el-dropdown-link">当前用户:{{sysUserName}}</span>
        <el-dropdown-menu slot="dropdown">
          <el-dropdown-item @click.native="logout">退出登录</el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
    </el-header>
    <el-container>
      <el-aside width="200px">
        <el-menu  router default-active="clientList" class="menu-list" style="height: 100%"
                  background-color="#545c64" text-color="#fff" >
          <el-menu-item index="datasourceList" route="" >数据源列表</el-menu-item>
          <el-menu-item index="clientList" route="">应用列表</el-menu-item>
          <el-menu-item index="queueMonitoring" route="" >队列监控</el-menu-item>
          <el-menu-item index="logProgress" route="" >日志进度</el-menu-item>
        </el-menu>
      </el-aside>
      <el-main>
        <el-col :span="24" class="breadcrumb-container">
          <strong class="title">
            <el-breadcrumb separator="/" class="breadcrumb-inner" style="float: left">
              <el-breadcrumb-item v-for="item in $route.matched" :key="item.path">
                {{ item.name }}
              </el-breadcrumb-item>
            </el-breadcrumb>
          </strong>
        </el-col>
        <el-col :span="24" class="content-wrapper" style="margin-top: 10px">
          <transition name="fade" mode="out-in">
            <router-view v-if="isRouterAlive"></router-view>
          </transition>
        </el-col>
      </el-main>
    </el-container>
  </el-container>
</template>
<script>
  import {data} from '../HttpInterceptors'
  export default {
    data(){
      return{
        sysUserName: '',
        namespaces: ['default', 'dev'],
        namespace: data.namespace,
        isRouterAlive: true
      }
    },
    watch: {
      namespace (val) {
        data.namespace = val
      }
    },
    methods:{
      logout () {
        var _this = this;
        this.$confirm('确认退出吗?', '提示', {
          //type: 'warning'
        }).then(() => {
          this.$cookies.remove("track_token");
          sessionStorage.removeItem('user');
          _this.$router.push('/login');
        }).catch(() => {
        });
      },
      reload () {
        this.isRouterAlive = false
        this.$nextTick(function () {
          this.isRouterAlive = true
        })
      }
    },
    mounted(){
      var user = sessionStorage.getItem('user');
      if (user) {
        user = JSON.parse(user);
        this.sysUserName = user.username || '';
      }
    }
  }

</script>
<style>
  .rightTitle{
    text-align: left;
    margin-top: 5px;
    margin-left: 1400px;
    font-family:"Nirmala UI";
    font-size: 15px;
  }
  .header{
    text-align: left;margin-top: 5px;
    font-family: Garamond;
    font-size: 40px;
    margin-left: 20px;
  }
  .el-header{
    /*background-color: #B3C0D1;*/
    /*color: #333;*/
    text-align: center;
    line-height: 60px;
    position: absolute;
    width: 100%;
    top: 0px;
    right: 0px;
  }

  .el-aside {
    /*background-color: #D3DCE6;*/
    /*color: #333;*/
    text-align: center;
    line-height: 200px;
    top: 60px;
    position: absolute;
    left: 0px;
    bottom: 0px;
  }

  .el-main {
    /*background-color: #E9EEF3;*/
    /*color: #333;*/
    /*line-height: 300px;*/
    position: absolute;
    bottom: 0px;
    top: 60px;
    left: 200px;
    right: 0px;
  }
</style>
