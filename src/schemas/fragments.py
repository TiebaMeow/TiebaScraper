from typing import Literal, get_args

from pydantic import BaseModel, field_validator


class FragAtModel(BaseModel):
    """@碎片模型。

    Attributes:
        type: 片段类型，固定为'at'
        text (str): 被@用户的昵称 含@
        user_id (int): 被@用户的user_id
    """

    type: Literal["at"] = "at"
    text: str = ""
    user_id: int = 0


class FragEmojiModel(BaseModel):
    """表情碎片模型。

    Attributes:
        type: 片段类型，固定为'emoji'
        id (str): 表情图片id
        desc (str): 表情描述
    """

    type: Literal["emoji"] = "emoji"
    id: str = ""
    desc: str = ""


class FragImageModel(BaseModel):
    """图像碎片模型。

    Attributes:
        type: 片段类型，固定为'image'
        src (str): 小图链接 宽720px
        big_src (str): 大图链接 宽960px
        origin_src (str): 原图链接
        origin_size (int): 原图大小（字节）
        show_width (int): 图像在客户端预览显示的宽度（像素）
        show_height (int): 图像在客户端预览显示的高度（像素）
        hash (str): 百度图床hash
    """

    type: Literal["image"] = "image"
    src: str = ""
    big_src: str = ""
    origin_src: str = ""
    origin_size: int = 0
    show_width: int = 0
    show_height: int = 0
    hash: str = ""


class FragItemModel(BaseModel):
    """item碎片模型。

    Attributes:
        type: 片段类型，固定为'item'
        text (str): item名称
    """

    type: Literal["item"] = "item"
    text: str = ""


class FragLinkModel(BaseModel):
    """链接碎片模型。

    Attributes:
        type: 片段类型，固定为'link'
        text (str): 原链接
        title (str): 链接标题
        raw_url (str): 解析后的原链接
    """

    type: Literal["link"] = "link"
    text: str = ""
    title: str = ""
    raw_url: str = ""

    @field_validator("raw_url", mode="before")
    @classmethod
    def _coerce_raw_url(cls, v):
        return "" if v is None else str(v)


class FragTextModel(BaseModel):
    """纯文本碎片模型。

    Attributes:
        type: 片段类型，固定为'text'
        text (str): 文本内容
    """

    type: Literal["text"] = "text"
    text: str = ""


class FragTiebaPlusModel(BaseModel):
    """贴吧plus广告碎片模型。

    Attributes:
        type: 片段类型，固定为'tieba_plus'
        text (str): 贴吧plus广告描述
        url (str): 解析后的贴吧plus广告跳转链接
    """

    type: Literal["tieba_plus"] = "tieba_plus"
    text: str = ""
    url: str = ""

    @field_validator("url", mode="before")
    @classmethod
    def _coerce_url(cls, v):
        return "" if v is None else str(v)


class FragVideoModel(BaseModel):
    """视频碎片模型。

    Attributes:
        type: 片段类型，固定为'video'
        src (str): 视频链接
        cover_src (str): 封面链接
        duration (int): 视频长度（秒）
        width (int): 视频宽度（像素）
        height (int): 视频高度（像素）
        view_num (int): 浏览次数
    """

    type: Literal["video"] = "video"
    src: str = ""
    cover_src: str = ""
    duration: int = 0
    width: int = 0
    height: int = 0
    view_num: int = 0


class FragVoiceModel(BaseModel):
    """音频碎片模型。

    Attributes:
        type: 片段类型，固定为'voice'
        md5 (str): 音频md5
        duration (int): 音频长度（秒）
    """

    type: Literal["voice"] = "voice"
    md5: str = ""
    duration: int = 0


class FragUnknownModel(BaseModel):
    """未知碎片模型。"""

    type: Literal["unknown"] = "unknown"
    raw_data: str = ""


Fragment = (
    FragAtModel
    | FragEmojiModel
    | FragImageModel
    | FragItemModel
    | FragLinkModel
    | FragTextModel
    | FragTiebaPlusModel
    | FragVideoModel
    | FragVoiceModel
    | FragUnknownModel
)

FRAG_MAP: dict[str, type[Fragment]] = {
    key: cls
    for cls in get_args(Fragment)
    for key in (cls.__name__.removesuffix("Model"), cls.model_fields["type"].default)
}
