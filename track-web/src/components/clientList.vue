<template>
  <div>
    <div style="margin-bottom: 10px; margin-right: 50px; display: flex; justify-content: flex-end">
      <el-button type="primary" size="small" @click="showCreator">
        添加应用
      </el-button>
    </div>

    <el-collapse v-model="activeClientGroup">
      <el-collapse-item v-for="(clientList, groupKey) in clientMap" :key="groupKey" :name="groupKey">
        <template slot="title">
          <span style="font-size: 18px;">{{groupKey}}</span>
        </template>
        <el-table :data="clientList">
          <el-table-column prop="name" label="应用名称" align="center"></el-table-column>
          <el-table-column prop="dsName" label="数据源" align="center"></el-table-column>
          <el-table-column prop="dbName" label="数据库" align="center"></el-table-column>
          <el-table-column prop="tableName" label="表" align="center"></el-table-column>
          <el-table-column  label="动作" align="center" width="260">
            <template slot-scope="scope">
              <el-tag v-for="eventAction in scope.row.eventActions" :key="eventAction" effect="plain" style="margin: 0 5px">
                {{eventAction}}
              </el-tag>
            </template>
          </el-table-column>
<!--          <el-table-column prop="queueType" label="队列类型" align="center"></el-table-column>-->
          <el-table-column label="操作" align="center">
            <template slot-scope="scope">
              <el-button type="warning" size="small" @click="showEditor(scope.row)">修改</el-button>
              <el-button type="danger" size="small" @click="deleteClient(scope.row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

      </el-collapse-item>
    </el-collapse>

    <el-dialog width="40%" :title="editorTitle" :visible.sync="addClientVisible">
      <el-form :model="client" :rules="rules" ref="ruleForm" label-width="100px">
        <el-form-item prop="name" label="应用名称" v-if="isCreate">
          <el-input v-model="client.name" class="auto"></el-input>
        </el-form-item>
        <el-form-item prop="dsName" label="数据源名称" v-if="isCreate">
          <el-select v-model="client.dsName" style="width: 100%" placeholder="请选择">
            <el-option
              v-for="item in dsNames"
              :key="item"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item prop="dbName" label="数据库">
          <el-input v-model="client.dbName" class="auto"></el-input>
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
<!--        <el-form-item label="队列类型" prop="queueType">-->
<!--          <el-radio-group v-model="client.queueType" @change="queueTypeChange">-->
<!--            <el-radio :label="'rabbit'">rabbit</el-radio>-->
<!--            <el-radio :label="'kafka'">kafka</el-radio>-->
<!--            <el-radio :label="'redis'">redis</el-radio>-->
<!--          </el-radio-group>-->
<!--        </el-form-item>-->
        <el-form-item prop="partitions" v-show="isKafkaQueueType" label="kafka分区">
          <el-input v-model="client.partitions" class="auto"></el-input>
        </el-form-item>
        <el-form-item prop="replication" v-show="isKafkaQueueType" label="kafka副本">
          <el-input v-model="client.replication" class="auto"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="addClientVisible = false">取 消</el-button>
        <el-button type="primary" size="small" @click="save('ruleForm')">确 定</el-button>
      </div>
    </el-dialog>

  </div>
</template>
<script>
import {
  saveClient,
  deleteClient,
  getClientMap,
  deleteTopic,
  getDatasourceNames,
} from '../api/api'
  import ElButton from "../../node_modules/element-ui/packages/button/src/button.vue";
  export  default {
    components: {ElButton},
    data() {
      return {
        addClientVisible: false,
        isCreate: false,
        isEdit: false,
        clientMap: {},
        activeClientGroup: [],
        client:{},
        defaultClient: {
          name: '',
          dsName: '',
          dbName: '',
          tableName: '',
          eventActions: ['INSERT', 'UPDATE', 'DELETE'],
          queueType: 'rabbit',
          replication: '',
          partitions: ''
        },
        rules: {
          name: [{required: true, message: '请输入应用名称', trigger: 'blur'}],
          dsName: [{required: true, message: '请选择数据源', trigger: 'blur'}],
          dbName: [{required: true, message: '请输入数据库名', trigger: 'blur'}],
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
      showCreator() {
        this.addClientVisible = true
        this.isCreate = true;
        this.client = {...this.defaultPersistDatasource}
        this.editorTitle = "添加应用";
      },
      showEditor(row) {
        this.addClientVisible = true
        this.isEdit = true;
        this.client = row
        this.editorTitle = "修改应用";
      },
      deleteClient(client){
        deleteClient(client).then(data=>{
          if(data.code == "success"){
            this.$message({
              type: 'success',
              message: '删除成功'
            })
            this.listClientMap()
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
      save(ruleForm) {
        this.$refs[ruleForm].validate((valid) => {
          if (valid) {
            saveClient(this.client).then((res)=> {
              if (res.data.code == 'success') {
                this.$message({
                  type: 'success',
                  message: '保存成功'
                })
                this.addClientVisible = false;
                this.listClientMap()
              }
              else {
                this.$message.error("保存失败：", res.data.msg)
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
      let token = this.$cookies.get("track_token");
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
