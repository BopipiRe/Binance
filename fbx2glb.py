import argparse
import logging
import os
import sys


def parse_args():
    # Blender 会将 `--` 后的参数传递给脚本，需手动截取
    argv = sys.argv
    if "--" not in argv:
        argv = []
    else:
        argv = argv[argv.index("--") + 1:]  # 提取 `--` 后的参数

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="输入FBX文件路径")
    parser.add_argument("--output", required=True, help="输出目录路径")
    return parser.parse_args(argv)


def setup_logging():
    """配置日志格式和级别"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
            logging.FileHandler("fbx_to_glb.log")  # 输出到文件
        ]
    )
    return logging.getLogger()


def fbx_to_glb(fbx_path, output_dir, logger):
    """将FBX文件转换为GLB格式，支持日志记录和参数化路径"""
    import bpy
    # 清除默认场景
    bpy.ops.object.select_all(action='SELECT')
    bpy.ops.object.delete()

    # 检查路径有效性
    if not os.path.isfile(fbx_path):
        logger.error(f"FBX文件不存在: {fbx_path}")
        raise FileNotFoundError(f"FBX文件不存在: {fbx_path}")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"创建输出目录: {output_dir}")

    # 处理大小写扩展名并生成输出路径
    filename = os.path.basename(fbx_path)
    glb_name = os.path.splitext(filename)[0] + ".glb"
    glb_path = os.path.join(output_dir, glb_name)

    try:
        # 导入FBX（关键参数优化）
        logger.info(f"开始导入FBX文件: {fbx_path}")
        bpy.ops.import_scene.fbx(
            filepath=fbx_path,
            global_scale=1.0,
            use_custom_normals=True,
            axis_forward='-Z',
            axis_up='Y'
        )
        logger.info("FBX导入完成")

        # 导出GLB
        logger.info(f"开始导出GLB文件: {glb_path}")
        bpy.ops.export_scene.gltf(
            filepath=glb_path,
            export_format='GLB',
            export_apply=True,
            export_animations=True,
            export_image_format='AUTO'
        )
        logger.info(f"转换成功: {glb_path}")
    except Exception as e:
        logger.error(f"转换失败: {e}", exc_info=True)
        raise
    finally:
        # 清理场景
        bpy.ops.object.select_all(action='SELECT')
        bpy.ops.object.delete()


if __name__ == "__main__":
    # 配置日志
    logger = setup_logging()
    args = parse_args()

    if not args.input or not args.output:
        logging.error("错误：缺少--input或--output参数")
        sys.exit(1)
    fbx_to_glb(args.input, args.output, logger)

    # input_fbx = "E:\\Code\\Python\\香城大饭店\\rc_huild_TBxcdjd_lod01.FBX"
    # output_dir = "D:\\glb_output"
    # fbx_to_glb(input_fbx, output_dir)
