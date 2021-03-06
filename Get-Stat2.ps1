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
    .PARAMETER threads
      Amount of threads to use. Should be less or equal to number of logical CPUs multiplied by 2.
    .PARAMETER SkipInstanceCheck
      Skip instances validation.
    .NOTES
       Original author:  Luc Dekens
       Modification author:  Andrey Nevedomskiy

       Updated 13.04.2015 by Nevedomskiy Andrey (monosoul):
       - Added ability to get metrics for multiple entities at a time.
       - Added parallel results parsing via runspaces.
       Updated 22.04.2015 by Nevedomskiy Andrey (monosoul):
       - Optimized parallel output parsing, now it would be up to 2x faster.
       Updated 16.10.2015 by Nevedomskiy Andrey (monosoul):
       - Added ability to get metrics for multiple instances at a time.
       - Added ability to use asterisk (*) as instance like in Get-Stat by VMware.
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
    [String[]]$Instance = "",
    [ValidateSet("RT","HI1","HI2","HI3","HI4")]
    [String]$Interval = "RT",
    [int]$MaxSamples,
    [switch]$QueryMetrics,
    [switch]$QueryInstances,
    [int]$threads,
    [switch]$SkipInstanceCheck)

  $speclist = @()

  $perfMgr = Get-View (Get-View ServiceInstance).content.perfManager

  # Create performance counter hashtable
  $pcTable    = New-Object Hashtable
  $pcTableRev = New-Object Hashtable
  $keyTable   = New-Object Hashtable
  foreach($pC in $perfMgr.PerfCounter){
    if($pC.Level -ne 99){
      if(!$pctable.containskey($pC.GroupInfo.Key + "." + $pC.NameInfo.Key + "." + $pC.RollupType)){
        $pctable.Add(($pC.GroupInfo.Key + "." + $pC.NameInfo.Key + "." + $pC.RollupType),$pC.Key)
        $pcTableRev.Add($pC.Key,($pC.GroupInfo.Key + "." + $pC.NameInfo.Key + "." + $pC.RollupType))
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
  $unitarray = New-Object Hashtable
  foreach($st in $Stat){
    if($pcTable[$st] -eq $null){
      Throw "-Stat parameter $st is invalid."
    }
    $pcInfo = $perfMgr.QueryPerfCounter($pcTable[$st])
    $unitarray.add($pcTable[$st],$pcInfo[0].UnitInfo.Key)
  }

  ForEach ($item in @($Entity)) {
  Write-Verbose $item.Name

    # Test if entity is valid
    if ($EntityType) {
      Remove-Variable -Name EntityType -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
    }
    $EntityType = $item.GetType().Name
  
    if(!(("HostSystem",
          "VirtualMachine",
          "ClusterComputeResource",
          "Datastore",
          "ResourcePool") -contains $EntityType)) {
      Throw "-Entity parameters should be of type HostSystem, VirtualMachine, ClusterComputeResource, Datastore or ResourcePool"
    }

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
    if (!$SkipInstanceCheck.IsPresent) {
      # Test if stat is valid
      if ($InstancesList) {
        Remove-Variable -Name InstancesList -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      }
      if ($validInstances) {
        Remove-Variable -Name validInstances -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      }
      if ($st) {
        Remove-Variable -Name st -Force -ErrorAction SilentlyContinue -WarningAction SilentlyContinue -Confirm:$false
      }
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
          $givenInstances = {$Instance}.Invoke()
          foreach($metric in $metricId){
            if($metric.CounterId -eq $pcTable[$st]){
              if($metric.Instance -eq "") {$cInstance = '""'} else {$cInstance = $metric.Instance}
              $validInstances += $cInstance
              if($givenInstances.Contains($metric.Instance)){
                $givenInstances.Remove($metric.Instance) | Out-Null
              }
            }
          }
          if($givenInstances.Contains("*")){
            $givenInstances.Remove("*") | Out-Null
          }
          if ($givenInstances.Count -eq 0) {$found = $true}
          if(!$found){
            [string]$stringInstance = ""
            $givenInstances | %{
              $stringInstance += $_.ToString() + ", "
            }
            $stringInstance = $stringInstance -replace ", $",""
            Throw "-Instance parameter ($stringInstance) invalid for requested stat: $st.`nValid values are: $validInstances"
          }
          if ($stringInstance) {
            Remove-Variable -Name stringInstance -Force -Confirm:$false
          }
          if ($givenInstances) {
            Remove-Variable -Name givenInstances -Force -Confirm:$false
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
      if (!([System.Array]::Equals($Instance, $null))) {
        $Instance | ForEach-Object {
          $PMId = New-Object VMware.Vim.PerfMetricId
          $PMId.counterId = $pcTable[$st]
          $PMId.instance = $_
          $PQSpec.MetricId += $PMId
        }
      } else {
        $PMId = New-Object VMware.Vim.PerfMetricId
        $PMId.counterId = $pcTable[$st]
        $PQSpec.MetricId += $PMId
      }
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

  # determining amount of logical CPU
  [int]$cpunum = 0
  (Get-WmiObject -Class Win32_processor).NumberOfLogicalProcessors | ForEach-Object {
    $cpunum += $_
  }
  # setting throttle to amount of logical CPUs * 2
  $throttle = ($cpunum * 2)
  if ($threads -and ($threads -le ($throttle))) {
    $throttle = $threads
  }

  # getting amount of stats per thread
  $limit = [math]::Ceiling(($Stats.Count / $throttle))

  # grouping stats by stats per thread
  $groups = @()
  [int]$skip = 0
  1..$throttle | %{
    $groups += New-Object PSObject -Property @{
      Name = $_
      Value = ($Stats | Select *,@{N="EntityName";E={$id = $_.Entity; ($Entity | ?{ $_.MoRef -eq $id }).Name}} -Skip $skip -First $limit)
    }
    $skip += $limit
  }

  Remove-Variable -Name Stats -Force -Confirm:$false
  [System.GC]::Collect()

  # parallelizing output parsing
  $RunspacePool = [RunspaceFactory]::CreateRunspacePool(1, $throttle)
  $RunspacePool.Open()

  $RS_scriptblock = {
    param(
      [PSObject]$stats,
      [PSObject]$pcTableRev,
      [PSObject]$unitarray
    )
    $data = @()
    ForEach ($stats_item in $stats) {
      for($j = 0; $j -lt $stats_item.Value.Count; $j ++ ){
        $script:valuecounter = 0
        $data += $stats_item.Value[$j].Value | Select `
          @{N="Timestamp";E={$stats_item.SampleInfo[$script:valuecounter].Timestamp}},`
          @{N="Interval";E={$stats_item.SampleInfo[$script:valuecounter].Interval}},`
          @{N="Value";E={$_;$script:valuecounter++}},`
          @{N="CounterName";E={$pcTableRev[$stats_item.Value[$j].Id.CounterId]}},`
          @{N="CounterId";E={$stats_item.Value[$j].Id.CounterId}},`
          @{N="Instance";E={$stats_item.Value[$j].Id.Instance}},`
          @{N="Unit";E={$unitarray[$stats_item.Value[$j].Id.CounterId]}},`
          @{N="Entity";E={$stats_item.EntityName}},`
          @{N="EntityId";E={$stats_item.Entity.ToString()}}
      }
    }
    return $data
  }

  $Jobs = New-Object System.Collections.ArrayList
  
  ForEach ($group in $groups) {
    $Job = [powershell]::Create().AddScript($RS_scriptblock)

    $Job.AddArgument($group.Value) | Out-Null
    $Job.AddArgument($pcTableRev) | Out-Null
    $Job.AddArgument($unitarray) | Out-Null

    $Job.RunspacePool = $RunspacePool
    $Jobs.Add((New-Object PSObject -Property @{
      Pipe = $Job
      Result = $Job.BeginInvoke()
    })) | Out-Null
  }

  Remove-Variable -Name group -Force -Confirm:$false
  Remove-Variable -Name groups -Force -Confirm:$false
  Remove-Variable -Name limit -Force -Confirm:$false
  Remove-Variable -Name skip -Force -Confirm:$false

  [System.GC]::Collect()

  #Waiting for all jobs to end
  $counter = 0
  $jobs_count = @($Jobs).Count
  $data = @()
  Do {
    #saving results
    ForEach ($Job in $Jobs) {
      if ($Job.Result.IsCompleted) {
        $data += $Job.Pipe.EndInvoke($Job.Result)
        $Job.Pipe.dispose()
        $Job.Result = $null
        $Job.Pipe = $null
        $counter++
      }
    }
    #removing unused jobs (runspaces)
    $temphash = $Jobs.Clone()
    $temphash | Where-Object { $_.pipe -eq $null } | ForEach {
      $Jobs.Remove($_) | Out-Null
    }
    Remove-Variable -Name temphash -Force -Confirm:$false
    [System.GC]::Collect()
    Start-Sleep -Seconds 1
  } While ( $counter -lt $jobs_count )

  $RunspacePool.dispose()
  $RunspacePool.Close()
  [System.GC]::Collect()

  if($MaxSamples -eq 0){
    $data | Sort-Object -Property Timestamp -Descending
  }
  else{
    $data | Sort-Object -Property Timestamp -Descending | select -First $MaxSamples
  }
}
