<template>
  <div>
    <div>
      <el-button
        style="float:right; margin-right: 70px;"
        @click="addClientVisible = true">添加应用</el-button>
    </div>

    <el-collapse v-model="activeClientGroup">
      <el-collapse-item v-for="(clientList, groupKey) in clientMap" :key="groupKey" :name="groupKey">
        <template slot="title">
          <div style="text-align: left; font-size: 18px;">
            {{groupKey}}
          </div>
        </template>
        <div style="text-align: right; padding-right: 50px;">
          <el-button type="danger" @click="handleDeleteTopic(groupKey)">删除通道topic</el-button>
        </div>
        <el-table
          :data="clientList">
          <el-table-column prop="name" label="应用名称" align="center"></el-table-column>
          <el-table-column prop="dsName" label="命名空间" align="center"></el-table-column>
          <el-table-column prop="databaseName" label="数据库" align="center"></el-table-column>
          <el-table-column prop="tableName" label="表" align="center"></el-table-column>
          <el-table-column prop="eventAction" label="动作" align="center"></el-table-column>
          <el-table-column prop="queueType" label="队列类型" align="center"></el-table-column>
          <el-table-column label="操作" align="center">
            <template slot-scope="scope">
              <el-button @click="deleteClient(scope.row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

      </el-collapse-item>
    </el-collapse>

    <el-dialog width="40%" title="添加应用" :visible.sync="addClientVisible">
      <el-form :model="client" :rules="rules" ref="ruleForm" label-width="100px">
        <el-form-item prop="name" label="应用名称">
          <el-input v-model="client.name" class="auto"></el-input>
        </el-form-item>
        <el-form-item prop="dsName" label="数据源名称">
          <el-select v-model="client.dsName" style="width: 100%" placeholder="请选择">
            <el-option
              v-for="item in dsNames"
              :key="item"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item prop="databaseName" label="数据库">
          <el-input v-model="client.databaseName" class="auto"></el-input>
        </el-form-item>
        <el-form-item prop="tableName" label="表名">
          <el-input v-model="client.tableName" class="auto" ></el-input>
        </el-form-item>

        <el-form-item label="表操作" prop="eventAction">
          <el-checkbox-group v-model="client.eventActions" @change="checkBoxChange">
            <el-checkbox label="INSERT">增加操作</el-checkbox>
            <el-checkbox label="UPDATE">更新操作</el-checkbox>
            <el-checkbox label="DELETE">删除操作</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item label="队列类型" prop="queueType">
          <el-radio-group v-model="client.queueType" @change="queueTypeChange">
            <el-radio :label="'rabbit'">rabbit</el-radio>
            <el-radio :label="'kafka'">kafka</el-radio>
            <el-radio :label="'redis'">redis</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item prop="partitions" v-show="isKafkaQueueType" label="kafka分区">
          <el-input v-model="client.partitions" class="auto"></el-input>
        </el-form-item>
        <el-form-item prop="replication" v-show="isKafkaQueueType" label="kafka副本">
          <el-input v-model="client.replication" class="auto"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="emitClose">取 消</el-button>
        <el-button type="primary" @click="addclient('ruleForm')">确 定</el-button>
      </div>
    </el-dialog>

  </div>
</template>
<script>
import {addClient, deleteClient, getClientMap, deleteTopic, getDatasourceNames} from '../api/api'
  import ElButton from "../../node_modules/element-ui/packages/button/src/button.vue";
  export  default {
    components: {ElButton},
    data() {
      return {
        addClientVisible: false,
        clientMap: {},
        activeClientGroup: [],

        client: {
          name: '',
          dsName: '',
          databaseName: '',
          tableName: '',
          eventActions: ['INSERT', 'UPDATE', 'DELETE'],
          queueType: 'rabbit',
          replication: '',
          partitions: ''
        },
        rules: {
          name: [{required: true, message: '请输入应用名称', trigger: 'blur'}],
          dsName: [{required: true, message: '请选择数据源', trigger: 'blur'}],
          databaseName: [{required: true, message: '请输入数据库名', trigger: 'blur'}],
          tableName: [{required: true, message: '请输入表名', trigger: 'blur'}],
          eventActions: [{required: true, message: '请勾选事件类型', trigger: 'blur'}],
          queueType: [{required: true, message: '请选择队列类型', trigger: 'blur'}]
        },
        dsNames: [],
        isKafkaQueueType: false
      }
    },
    methods:{
      listClientMap() {
        getClientMap('').then((res)=>{
          this.clientMap=res.data;
          this.activeClientGroup = Object.keys(res.data)
        })
      },
      deleteClient(client){
        deleteClient(client).then(data=>{
          if(data.code=="success"){
            this.$message({
              type: 'success',
              message: '删除成功'
            })
            this.list()
          }else {
            this.$message.error("删除失败：", data.msg)
          }
        })
      },
      handleDeleteTopic(clientInfoKey) {
        this.$confirm('确认删除</br><strong>' + clientInfoKey +'</strong></br>对应通道的topic？', '删除确认', {dangerouslyUseHTMLString: true}).then(_ => {
          deleteTopic({clientInfoKey: clientInfoKey}).then(res => {
            console.log(res)
            if('success' == res.data.code) {
                this.$message({
                  message: res.data.msg,
                  type: 'success'
              })
            } else {
              this.$message({
                message: res.data.msg,
                type: 'error'
              })
            }
            this.listClientMap()
          })
        })
      },
      addclient(ruleForm) {
        this.$refs[ruleForm].validate((valid) => {
          if (valid) {
            addClient(this.client).then((res)=> {
              if (res.data.code == 'success') {
                this.$message({
                  type: 'success',
                  message: '添加成功'
                })
              }
              else {
                this.$message.error("添加失败：", res.data.msg)
              }
            })
          }
        })
      },
      checkBoxChange(list) {
        this.client.eventActions = list;
      },
      queueTypeChange() {
        this.isKafkaQueueType = this.client.queueType === 'kafka'
      },
    },
    mounted(){

      // sso
      let token = this.$cookies.get("keking_token");
      if(token) {
        sessionStorage.setItem('user', '{"username":"' + token.username + '","token":"' + token.access_token + '"}');
      }

      this.listClientMap()

      getDatasourceNames().then(res => {
        this.dsNames = res.data
      })
    }
  }
</script>
<style>

</style>
