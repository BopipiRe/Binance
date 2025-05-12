import os

def fbx_to_glb(fbx_path, output_path):
    # 清除默认场景
    import bpy
    bpy.ops.wm.read_factory_settings(use_empty=True)
    
    if fbx_path.lower().endswith(".fbx"):
        _, file = os.path.split(fbx_path)
        glb_path = os.path.join(output_path, file.replace(".fbx", ".glb").replace(".FBX", ".glb"))
        
        # 导入FBX
        bpy.ops.import_scene.fbx(filepath=fbx_path)
        
        # 导出GLB（二进制格式）
        bpy.ops.export_scene.gltf(
            filepath=glb_path,
            export_format='GLB',  # 指定为GLB格式
            export_apply=True     # 应用变换
        )
        
        # 清除当前场景以准备下一个文件
        bpy.ops.wm.read_factory_settings(use_empty=True)

if __name__ == "__main__":
    fbx_to_glb("E:\\Code\\Python\\香城大饭店\\rc_huild_TBxcdjd_lod01.FBX", "D:/glb_output")
