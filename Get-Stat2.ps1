function Get-Stat2 {
    <#
    .Synopsis
       Retrieve vSphere statistics
    .DESCRIPTION
       The function is an alternative to the Get-Stat cmdlet.
       It's primary use is to provide functionality that is missing
       from the Get-Stat cmdlet.
    .EXAMPLE
       PS> Get-Stat2 -Entity $vm.Extensiondata -Stat "cpu.usage.average" -Interval "RT"
    .PARAMETER Entity
      Specify the VIObject(s) for which you want to retrieve statistics
      This needs to be an SDK object
    .PARAMETER Start
      Start of the interval for which to retrive statistics
    .PARAMETER Finish
      End of the interval for which to retrive statistics
    .PARAMETER Stat
      The identifiers of the metrics to retrieve
    .PARAMETER Instance
      The instance property of the statistics to retrieve
    .PARAMETER Interval
      Specify for which interval you want to retrieve statistics.
      Allowed values are RT, HI1, HI2, HI3 and HI4
    .PARAMETER MaxSamples
      The maximum number of samples for each metric
    .PARAMETER QueryMetrics
      Switch to indicate that the function should return the available
      metrics for the Entity specified
    .PARAMETER QueryInstances
      Switch to indicate that the function should return the valid instances
      for a specific Entity and Stat
    .NOTES
       Original author:  Luc Dekens
       Modification author:  Andrey Nevedomskiy

       Updated 13.04.2015 by Nevedomskiy Andrey (monosoul):
       - Added ability to get metrics for multiple entities at a time.
       - Added parallel results parsing via runspaces.
    .FUNCTIONALITY
       Retrieve vSphere statistics
   
    #>

  [CmdletBinding()]
  param (
    [parameter(Mandatory = $true, ValueFromPipeline = $true)]
    [PSObject]$Entity,
    [DateTime]$Start,
    [DateTime]$Finish,
    [String[]]$Stat,
    [String]$Instance = "",
    [ValidateSet("RT","HI1","HI2","HI3","HI4")]
    [String]$Interval = "RT",
    [int]$MaxSamples,
    [switch]$QueryMetrics,
    [switch]$QueryInstances)

  $speclist = @()

  $perfMgr = Get-View (Get-View ServiceInstance).content.perfManager

  # Create performance counter hashtable
  $pcTable = New-Object Hashtable
  $keyTable = New-Object Hashtable
  foreach($pC in $perfMgr.PerfCounter){
    if($pC.Level -ne 99){
      if(!$pctable.containskey($pC.GroupInfo.Key + "." + $pC.NameInfo.Key + "." + $pC.RollupType)){
        $pctable.Add(($pC.GroupInfo.Key + "." + $pC.NameInfo.Key + "." + $pC.RollupType),$pC.Key)
        $keyTable.Add($pC.Key, $pC)
      }
    }
  }

  # Test for a valid $Interval
  if($Interval.ToString().Split(" ").count -gt 1){
    Throw "Only 1 interval allowed."
  }
  
  $intervalTab = @{"RT"=$null;"HI1"=0;"HI2"=1;"HI3"=2;"HI4"=3}
  $dsValidIntervals = "HI2","HI3","HI4"
  $intervalIndex = $intervalTab[$Interval]
  
  if($EntityType -ne "datastore"){
    if($Interval -eq "RT"){
      $numinterval = 20
    }
    else{
      $numinterval = $perfMgr.HistoricalInterval[$intervalIndex].SamplingPeriod
    }
  }
  else{
    if($dsValidIntervals -contains $Interval){
      $numinterval = $null
      if(!$Start){
        $Start = (Get-Date).AddSeconds($perfMgr.HistoricalInterval[$intervalIndex].SamplingPeriod - $perfMgr.HistoricalInterval[$intervalIndex].Length)
      }
      if(!$Finish){
        $Finish = Get-Date
      }
    }
    else{
      Throw "-Interval parameter $Interval is invalid for datastore metrics."
    }
  }

  # Test if start is valid
  if($Start -ne $null -and $Start -ne ""){
    if($Start.gettype().name -ne "DateTime") {
      Throw "-Start parameter should be a DateTime value"
    }
  }
  
  # Test if finish is valid
  if($Finish -ne $null -and $Finish -ne ""){
    if($Finish.gettype().name -ne "DateTime") {
      Throw "-Start parameter should be a DateTime value"
    }
  }
  
  # Test start-finish interval
  if($Start -ne $null -and $Finish -ne $null -and $Start -ge $Finish){
    Throw "-Start time should be 'older' than -Finish time."
  }

  # Building unit array
  Remove-Variable -Name unitarray -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
  $unitarray = @()
  foreach($st in $Stat){
    if($pcTable[$st] -eq $null){
      Throw "-Stat parameter $st is invalid."
    }
    $pcInfo = $perfMgr.QueryPerfCounter($pcTable[$st])
    $unitarray += $pcInfo[0].UnitInfo.Key
  }

  ForEach ($item in @($Entity)) {
  Write-Verbose $item.Name

    # Test if entity is valid
    Remove-Variable -Name EntityType -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
    $EntityType = $item.GetType().Name
  
    if(!(("HostSystem",
          "VirtualMachine",
          "ClusterComputeResource",
          "Datastore",
          "ResourcePool") -contains $EntityType)) {
      Throw "-Entity parameters should be of type HostSystem, VirtualMachine, ClusterComputeResource, Datastore or ResourcePool"
    }
    
  
    # Test if QueryMetrics is given
    if($QueryMetrics){
      $metrics = $perfMgr.QueryAvailablePerfMetric($item.MoRef,$null,$null,$numinterval)
      $metricslist = @()
      foreach($pmId in $metrics){
        $pC = $keyTable[$pmId.CounterId]
        $metricslist += New-Object PSObject -Property @{
          Group = $pC.GroupInfo.Key
          Name = $pC.NameInfo.Key
          Rollup = $pC.RollupType
          Id = $pC.Key
          Level = $pC.Level
          Type = $pC.StatsType
          Unit = $pC.UnitInfo.Key
        }
      }
      return ($metricslist | Sort-Object -unique -property Group,Name,Rollup)
    }
  
    #if passed more than one entities then skip stat check to speed up process
    if (@($Entity).Count -eq 1) {
      # Test if stat is valid
      Remove-Variable -Name InstancesList -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      Remove-Variable -Name validInstances -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      Remove-Variable -Name st -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      $InstancesList = @()
  
      foreach($st in $Stat){
        $metricId = $perfMgr.QueryAvailablePerfMetric($item.MoRef,$null,$null,$numinterval)
  
        # Test if QueryInstances in given
        if($QueryInstances){
          $mKey = $pcTable[$st]
          foreach($metric in $metricId){
            if($metric.CounterId -eq $mKey){
              $InstancesList += New-Object PSObject -Property @{
                Stat = $st
                Instance = $metric.Instance
              }
            }
          }
        }
        else{
          # Test if instance is valid
          $found = $false
          $validInstances = @()
          foreach($metric in $metricId){
            if($metric.CounterId -eq $pcTable[$st]){
              if($metric.Instance -eq "") {$cInstance = '""'} else {$cInstance = $metric.Instance}
              $validInstances += $cInstance
              if($Instance -eq $metric.Instance){$found = $true}
            }
          }
          if(!$found){
            Throw "-Instance parameter invalid for requested stat: $st.`nValid values are: $validInstances"
          }
        }
      }
      if($QueryInstances){
        return $InstancesList
      }
    }
  
    $PQSpec = New-Object VMware.Vim.PerfQuerySpec
    $PQSpec.entity = $item.MoRef
    $PQSpec.Format = "normal"
    $PQSpec.IntervalId = $numinterval
    $PQSpec.MetricId = @()
    foreach($st in $Stat){
      $PMId = New-Object VMware.Vim.PerfMetricId
      $PMId.counterId = $pcTable[$st]
      if($Instance -ne $null){
        $PMId.instance = $Instance
      }
      $PQSpec.MetricId += $PMId
    }
    if ($Start) { $PQSpec.StartTime = $Start }
    if ($Finish) { $PQSpec.EndTime = $Finish }
    if($MaxSamples -eq 0 -or $numinterval -eq 20){
      $PQSpec.maxSample = $null
    }
    else{
      $PQSpec.MaxSample = $MaxSamples
    }
    $speclist += $PQSpec
  }

  $Stats = $perfMgr.QueryPerf($speclist)

  # No data available
  if($Stats[0].Value -eq $null) {return $null}

  $cpunum = (Get-WmiObject -Class Win32_processor).NumberOfLogicalProcessors

  # parallelizing output parsing
  $RunspacePool = [RunspaceFactory]::CreateRunspacePool(1, $($cpunum * 2))
  $RunspacePool.Open()

  $RS_scriptblock = {
    param(
      [PSObject]$stats_item,
      [PSObject]$Stat,
      [PSObject]$unitarray,
      [string]$entity_name
    )
    $data = @()
    for($i = 0; $i -lt $stats_item.SampleInfo.Count; $i ++ ){
      for($j = 0; $j -lt $Stat.Count; $j ++ ){
        $data += New-Object PSObject -Property @{
          CounterId = $stats_item.Value[$j].Id.CounterId
          CounterName = $Stat[$j]
          Instance = $stats_item.Value[$j].Id.Instance
          Timestamp = $stats_item.SampleInfo[$i].Timestamp
          Interval = $stats_item.SampleInfo[$i].Interval
          Value = &{if($stats_item.Value){
              $stats_item.Value[$j].Value[$i]
            }else{-1}
          }
          Unit = $unitarray[$j]
          Entity = $entity_name
          EntityId = $stats_item.Entity.ToString()
        }
      }
    }
    return $data
  }

  $Jobs = @()
  
  ForEach ($stats_item in $Stats) {
    $entity_name = ($Entity | Where-Object { $_.MoRef -eq $stats_item.Entity }).Name

    $Job = [powershell]::Create().AddScript($RS_scriptblock)

    $Job.AddArgument($stats_item) | Out-Null
    $Job.AddArgument($Stat) | Out-Null
    $Job.AddArgument($unitarray) | Out-Null
    $Job.AddArgument($entity_name) | Out-Null

    $Job.RunspacePool = $RunspacePool
    $Jobs += New-Object PSObject -Property @{
      Pipe = $Job
      Result = $Job.BeginInvoke()
    }
  }

  #Waiting for all jobs to end
  Do {
    Start-Sleep -Seconds 1
  } While ( $Jobs.Result.IsCompleted -contains $false )

  $data = @()

  ForEach ($Job in $Jobs) {
    $data += $Job.Pipe.EndInvoke($Job.Result)
  }

  $RunspacePool.Close()

  if($MaxSamples -eq 0){
    $data | Sort-Object -Property Timestamp -Descending
  }
  else{
    $data | Sort-Object -Property Timestamp -Descending | select -First $MaxSamples
  }
}
