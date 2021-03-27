package local

import (
  "fmt"
  fpmod "path/filepath"
  "sort"
  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
)

// Implement interface for sorter
type ByPath []*pb.SnapshotChanges_Change
func (a ByPath) Len() int           { return len(a) }
func (a ByPath) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPath) Less(i, j int) bool { return a[i].Path < a[j].Path }

// Rules (see https://docs.python.org/3/library/stdtypes.html#set-types-set-frozenset):
// * ToFrom'   = while not stable { ToFrom[k] = ToFrom[ToFrom[k].dirname] + ToFrom[k].basename }
// * FromTo'   = nil
// * New'      = (New - FromTo.keys)    | (FromTo[k] for k in New)
// * NewDir'   = (NewDir - FromTo.keys) | (FromTo[k] for k in NewDir)
// * Written'  = Written - New'
// * Move'     = (ToFrom' - Deleted - DelDir) & (FromTo - New - NewDir).values 
// * Deleted'  = (ToFrom'[k] for k in Deleted) | (ToFrom'[dir]/base for dir,base in Deleted) | remaining keys in Deleted
// * Deleted'' = Deleted' - Move'.values
// * DelDir'   = (ToFrom'[k] for k in DelDir)  | (ToFrom'[dir]/base for dir,base in DelDir)  | remaining keys in DelDir
// * DelDir''  = DelDir' - Move'.values
func sendDumpOpsToSnapChanges(state *types.SendDumpOperations) *pb.SnapshotChanges {
  if state.Err != nil { panic(fmt.Sprintf("SendDumpOperations has an error: %v", state.Err)) }
  to_from    := invert(state.FromTo)
  to_from_p  := replaceTempNamesInValues(to_from)
  new_p      := union(diff(state.New, state.FromTo), project(state.New, state.FromTo))
  newdir_p   := union(diff(state.NewDir, state.FromTo), project(state.NewDir, state.FromTo))
  written_p  := diff2(state.Written, new_p)
  move_p     := intersect(diff4(diff4(to_from_p, state.Deleted), state.DelDir),
                          invert(diff4(diff4(state.FromTo, state.New), state.NewDir)))
  deleted_p  := recursiveProjectWithRest(state.Deleted, to_from_p)
  deleted_pp := diff(deleted_p, invert(move_p))
  deldir_p   := recursiveProjectWithRest(state.DelDir, to_from_p)
  deldir_pp  := diff(deldir_p, invert(move_p))

  //runtime.Breakpoint()
  changes := make([]*pb.SnapshotChanges_Change, 0, 64)
  add_op_f := func(files map[string]bool, t pb.SnapshotChanges_Type) {
    for path,v := range files {
      if !v { continue }
      changes = append(changes, &pb.SnapshotChanges_Change {
        Type: t,
        Path: path,
      })
    }
  }
  add_op_f(new_p, pb.SnapshotChanges_NEW)
  add_op_f(newdir_p, pb.SnapshotChanges_NEW_DIR)
  add_op_f(deleted_pp, pb.SnapshotChanges_DELETE)
  add_op_f(deldir_pp, pb.SnapshotChanges_DEL_DIR)
  add_op_f(written_p, pb.SnapshotChanges_WRITE)
  for path,v := range move_p {
    changes = append(changes, &pb.SnapshotChanges_Change {
      Type: pb.SnapshotChanges_MOVE,
      Path: path,
      From: v,
    })
  }

  sort.Sort(ByPath(changes))
  proto := pb.SnapshotChanges{
    Changes: changes,
    ToUuid: state.ToUuid,
    FromUuid: state.FromUuid,
  }
  return &proto
}

func union(a map[string]bool, b map[string]bool) map[string]bool {
  res := make(map[string]bool)
  for k,_ := range a { res[k] = true }
  for k,_ := range b { res[k] = true }
  return res
}

// No overload nor generics ....
func diff(a map[string]bool, b map[string]string) map[string]bool {
  res := make(map[string]bool)
  for k,_ := range a {
    if _, found := b[k]; !found { res[k] = true }
  }
  return res
}
func diff2(a map[string]bool, b map[string]bool) map[string]bool {
  res := make(map[string]bool)
  for k,_ := range a {
    if _, found := b[k]; !found { res[k] = true }
  }
  return res
}
func diff4(a map[string]string, b map[string]bool) map[string]string {
  res := make(map[string]string)
  for k,v := range a {
    if _, found := b[k]; !found { res[k] = v }
  }
  return res
}

func intersect(a map[string]string, b map[string]string) map[string]string {
  res := make(map[string]string)
  for k,v := range a {
    if _, found := b[k]; found { res[k] = v }
  }
  return res
}

func project(a map[string]bool, b map[string]string) map[string]bool {
  res := make(map[string]bool)
  for k,_ := range a {
    if v, found := b[k]; found { res[v] = true }
  }
  return res
}

func recursiveProjectWithRest(a map[string]bool, b map[string]string) map[string]bool {
  res := make(map[string]bool)
  for k,_ := range a {
    translated := false
    dirname := k
    basename := ""
    visit_paths:for dirname != "." {
      orig_path, found := b[dirname]
      if found {
        res[fpmod.Join(orig_path, basename)] = true
        translated = true
        break visit_paths
      }
      basename = fpmod.Join(fpmod.Base(dirname), basename)
      dirname = fpmod.Dir(dirname)
    }
    if !translated { res[k] = true }
  }
  return res
}

// Assert all values are different
func invert(kv map[string]string) map[string]string {
  dst_kv := make(map[string]string)
  for k,v := range kv {
    if _, found := dst_kv[k]; found {
      panic(fmt.Sprintf("not all values are distinct: %v", kv))
    }
    dst_kv[v] = k
  }
  return dst_kv
}

func replaceTempNamesInValues(to_from map[string]string) map[string]string {
  dst_kv := make(map[string]string)
  for k,v := range to_from { dst_kv[k] = v }

  for keep,iteration_cnt := 1,0; keep > 0; keep,iteration_cnt = 0,iteration_cnt+1 {
    for k,v := range dst_kv {
      dirname := fpmod.Dir(v)
      basename := fpmod.Base(v)
      // So far I have not seen patterns like
      // temp_1 -> orig_1
      // temp_2 -> temp_1/orig_2
      // temp_3 -> temp_1/orig_2/orig_3
      // But instead always just 1 dir level
      // temp_1 -> orig_1
      // temp_2 -> temp_1/orig_2
      // temp_3 -> temp_2/orig_3
      visit_paths:for dirname != "." {
        orig_path, found := dst_kv[dirname]
        if found {
          dst_kv[k] = fpmod.Join(orig_path, basename)
          keep += 1
          break visit_paths
        }
        basename = fpmod.Join(fpmod.Base(dirname), basename)
        dirname = fpmod.Dir(dirname)
      }
    }
    if iteration_cnt > 5 {
      panic(fmt.Sprintf("too many iterations to resolve paths in: %v", to_from))
    }
  }
  return dst_kv
}


