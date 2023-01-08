<script src="../api/api.js"></script>
<template>
  <div>
    <el-button
      style="float:right; margin-right: 70px;"
      @click="showCreator">添加数据源</el-button>
    <div style="float:right; margin-right: 50px; ">
      <span>刷新间隔</span>
      <el-select
        style="width: 80px; margin-left: 20px;"
        v-model="refreshInterval"
        placeholder="请选择">
        <el-option
          v-for="item in refreshOptions"
          :key="item.value"
          :label="item.label"
          :value="item.value">
        </el-option>
      </el-select>
    </div>

    <el-table :data="datasourceList">
      <el-table-column prop="name" label="名称" align="center"/>
      <el-table-column prop="destQueue" label="目标队列" align="center"/>
      <el-table-column prop="dataSourceType" label="数据源类型" align="center"/>
      <el-table-column prop="dataSourceUrl" label="url" align="center"/>
      <el-table-column label="状态" align="center">
        <template slot-scope="scope">
          <el-tag v-if="scope.row.active" type="success">运行中</el-tag>
          <el-tag v-if="!scope.row.active" type="danger">已关闭</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" min-width="150px">
        <template slot-scope="scope">
          <el-button
            v-if="!scope.row.active"
            @click="clickStart(scope.row.name)">
            开启
          </el-button>
          <el-button
            v-if="scope.row.active"
            @click="handleStopDatasource(scope.row.name)">关闭
          </el-button>
          <el-button
            v-if="!scope.row.active"
            @click="showEditor(scope.row)">修改
          </el-button>
          <el-button
            v-if="!scope.row.active"
            @click="handleRemoveDatasource(scope.row.name)">移除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <div style="padding: 40px 0;">
    </div>

    <el-table :data="serviceStatusList">
      <el-table-column prop="ip" label="IP" align="center"/>
      <el-table-column prop="activeDsCount" label="有效的命名空间" align="center"/>
      <el-table-column prop="totalEventCount" label="Event总数量" align="center"/>
      <el-table-column prop="latelyEventCount" label="Event间隔数量" align="center"/>
      <el-table-column prop="totalPublishCount" label="Publish总数量" align="center"/>
      <el-table-column prop="latelyPublishCount" label="Publish间隔数量" align="center"/>
      <el-table-column prop="updateTime" label="更新时间" align="center"/>
    </el-table>

    <el-dialog width="40%" :title="editorTitle" :visible.sync="editorVisible">
      <el-form :rules="rules" ref="ruleForm" class="persist-datasource-form" label-width="150px"
               :model="persistDatasource">
        <el-form-item label="名称" prop="name" style="width: 50%">
          <el-input class="form-input" v-model="persistDatasource.name"/>
        </el-form-item>
        <el-form-item label="数据源类型" prop="dataSourceType">
          <el-select v-model="persistDatasource.dataSourceType" placeholder="请选择">
            <el-option v-for="item in dataSourceTyepOptions"
                       :key="item.value"
                       :label="item.label"
                       :value="item.value"/>
          </el-select>
        </el-form-item>
        <el-form-item label="目标队列" prop="dataSourceType">
          <el-select v-model="persistDatasource.destQueue" placeholder="请选择">
            <el-option v-for="item in destQueues"
                       :key="item.value"
                       :label="item.label"
                       :value="item.value"/>
          </el-select>
        </el-form-item>
        <el-form-item label="连接URL" prop="dataSourceUrl">
          <el-input class="form-input" type="textarea" :autosize="{ minRows: 4, maxRows: 5}"
                    v-model="persistDatasource.dataSourceUrl"/>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="editorVisible = false">取 消</el-button>
        <el-button type="primary" @click="handlePersistDatasource">确 定</el-button>
      </div>
    </el-dialog>

    <el-dialog width="30%" title="开启数据源" :visible.sync="startVisible">
      <el-form ref="ruleForm" class="persist-datasource-form" label-width="150px" :model="persistDatasource">
        <el-form-item label="指定IP" prop="name">
          <el-select
            style=";"
            v-model="startDataSourceParams.delegatedIp"
            placeholder="请选择">
            <el-option
              v-for="item in serviceStatusList"
              :key="item.ip"
              :label="item.ip"
              :value="item.ip">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="startVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleStartDatasource">确 定</el-button>
      </div>
    </el-dialog>

  </div>
</template>
<script>
  import {getDatasourceList, stopDatasource, startDatasource, persistDatasource, updateDatasource, removeDatasource, getServiceStatus} from '../api/api'
  import ElButton from "../../node_modules/element-ui/packages/button/src/button.vue";
  export  default {
    components: {ElButton},
    data(){
      return{
        datasourceList:[],
        serviceStatusList: [],
        editorTitle: "添加数据源",
        editorVisible: false,
        startVisible: false,
        persistDatasource: {
          name: null,
          destQueue: 'rabbit',
          dataSourceType: 'MongoDB',
          dataSourceUrl: 'mongodb://root:root@localhost:27017/?replicaSet=replicaset&authSource=admin',
        },
        defaultPersistDatasource: {
          destQueue: 'rabbit',
          dataSourceType: 'MongoDB',
          dataSourceUrl: 'mongodb://root:root@localhost:27017/?replicaSet=replicaset&authSource=admin',
        },
        dataSourceTyepOptions: [{
          value: 'MongoDB',
          label: 'MongoDB'
        }],
        destQueues: [{
          value: 'rabbit',
          label: 'rabbit'
        },{
          value: 'kafka',
          label: 'kafka'
        }],
        startDataSourceParams: {
          name: '',
          delegatedIp: '',
        },
        refreshOptions: [
          {value: 1, label: '1s'},
          {value: 3, label: '3s'},
          {value: 5, label: '5s'},
          {value: 10, label: '10s'},
          {value: 20, label: '20s'},
          {value: 30, label: '30s'}
        ],
        rules: {
          name: [
            {required: true, message: '请输入', trigger: 'blur'},
          ],
          dataSourceType: [
            {required: true, message: '请输入', trigger: 'blur'},
          ],
          dataSourceUrl: [
            { required: true, message: '请输入', trigger: 'blur' },
          ],
        },
        refreshInterval: 5,
        refreshTimer: 0,
      }
    },
    watch: {
      refreshInterval: function() {
        this.refreshConfig()
      }
    },
    methods:{
      list(){
        getDatasourceList('').then((res) => {
          this.datasourceList = res.data;
        });
        getServiceStatus().then(res => {
          this.serviceStatusList = res.data
        })
      },
      showCreator() {
        this.editorVisible = true
        this.persistDatasource = {...this.defaultPersistDatasource}
        this.editorTitle = "添加数据源";
      },
      showEditor(row) {
        this.editorVisible = true
        this.persistDatasource = row
        this.editorTitle = "修改数据源";
      },
      clickStart(name) {
        this.startVisible = true;
        this.startDataSourceParams.name = name;
      },
      handleStartDatasource(){
        this.startVisible = false;
        startDatasource(this.startDataSourceParams).then(res=>{
          if (res.data.code === "success") {
            this.$message({
              type: 'success',
              message: res.data.msg
            })
          } else {
            this.$message.error(res.data.msg)
          }
        })
      },
      handleStopDatasource(name){
        stopDatasource({
          name: name
        }).then(res=>{
          if (res.data.code === "success") {
            this.$message({
              type: 'success',
              message: '关闭数据源监听成功'
            })
          } else {
            this.$message.error("关闭数据源监听失败")
          }
        })
      },
      handlePersistDatasource() {
        this.$refs['ruleForm'].validate((valid) => {
          if(!valid) {
            return
          }

          this.editorVisible = false;
          const action = this.persistDatasource.id != null ? updateDatasource : persistDatasource
          action(this.persistDatasource).then(res=>{
            if(res.data.code==="success"){
              this.$message({
                type: 'success',
                message: '添加数据源成功'
              });
              this.list();
              this.persistDatasource = this.defaultPersistDatasource
            }else {
              this.$message.error("添加数据源失败")
            }
          })
        })
      },
      handleRemoveDatasource(name){
        removeDatasource({
          name: name
        }).then(res=>{
          if(res.data.code==="success"){
            this.$message({
              type: 'success',
              message: '移除数据源成功'
            });
            this.list()
          }else {
            this.$message.error("数据源监听失败")
          }
        })
      },
      refresh() {
        clearInterval(this.refreshTimer);
        let refreshIntervalMs = this.refreshInterval*1000;
        this.refreshTimer = setInterval(() => {
          this.list();
        }, (refreshIntervalMs))
      }
    },
    mounted(){
      this.list();
      this.refresh()
    },
    destroyed() {
      clearInterval(this.refreshTimer)
    }
  }
</script>
<style type="scss" scoped>


</style>
