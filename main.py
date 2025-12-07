"""
Основной модуль приложения для обработки месячных данных.

Собирает данные из файлов в каталогах OD, RA, PS,
формирует сводный файл с уникальными табельными номерами
и сохраняет результат в форматированном Excel файле.

Все настройки и конфигурация находятся в этом файле.
"""

import logging
import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

import pandas as pd

# Попытка импортировать openpyxl для форматирования (обычно доступен в Anaconda)
try:
    from openpyxl import load_workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

# Попытка импортировать xlsxwriter для форматирования (если openpyxl недоступен)
try:
    import xlsxwriter
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False


# ============================================================================
# НАСТРОЙКИ ПРИЛОЖЕНИЯ
# ============================================================================

# Пути к каталогам
INPUT_DIR = "IN"  # Каталог с входными данными
OUTPUT_DIR = "OUT"  # Каталог для выходных файлов
LOG_DIR = "log"  # Каталог для логов

# Уровень логирования (INFO или DEBUG)
LOG_LEVEL = "INFO"  # Измените на "DEBUG" для детального логирования

# Тема логов (используется в имени файла)
LOG_THEME = "processor"


# ============================================================================
# КОНФИГУРАЦИЯ ЗАГРУЗКИ ФАЙЛОВ
# ============================================================================

@dataclass
class DropRule:
    """
    Правило удаления строк.
    
    Параметры:
        alias: Имя поля после маппинга (из default_columns, например "tb", "status")
        values: Список запрещенных значений (строки будут удалены, если значение поля совпадает с одним из них)
        remove_unconditionally: True - удалять всегда (по умолчанию), False - не удалять (правило игнорируется)
        check_by_inn: True - не удалять строку, если по этому ИНН (client_id) есть другие строки с незапрещенными значениями
        check_by_tn: True - не удалять строку, если по этому ТН (tab_number) есть другие строки с незапрещенными значениями
    
    Комбинации:
        - remove_unconditionally=True, check_by_inn=False, check_by_tn=False: удаляем ВСЕ строки с запрещенными значениями
        - remove_unconditionally=True, check_by_inn=True: удаляем, НО если по ИНН есть другие значения - не удаляем
        - remove_unconditionally=True, check_by_tn=True: удаляем, НО если по ТН есть другие значения - не удаляем
        - remove_unconditionally=True, check_by_inn=True, check_by_tn=True: удаляем, НО если по ИНН ИЛИ по ТН есть другие значения - не удаляем (логика ИЛИ)
        - remove_unconditionally=False: строки НЕ удаляются, правило игнорируется
    """
    alias: str
    values: List[str]
    remove_unconditionally: bool = True
    check_by_inn: bool = False
    check_by_tn: bool = False


@dataclass
class IncludeRule:
    """
    Правило включения строк.
    
    Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (логика И).
    И при этом НЕ попадает под drop_rules (исключается из DROP).
    
    Параметры:
        alias: Имя поля после маппинга (из default_columns, например "type", "tb")
        values: Список значений для проверки
        condition: "in" - значение должно быть в списке values, "not_in" - значение НЕ должно быть в списке
    
    Примеры:
        IncludeRule(alias="type", values=["Активен"], condition="in") - только строки с type="Активен"
        IncludeRule(alias="tb", values=["ЦА"], condition="not_in") - только строки где tb НЕ равно "ЦА"
    """
    alias: str
    values: List[str]
    condition: str = "in"  # "in" или "not_in"


@dataclass
class FileItem:
    """
    Элемент конфигурации для одного файла.
    
    Структура соответствует примеру из репозитория:
    - key: ключ файла (для идентификации)
    - label: подпись для логов
    - file_name: имя файла в каталоге IN (если пустое "", файл не используется)
    - sheet: название листа (если None, используется default_sheet из группы)
    - columns: список колонок (если пустой [], используются из defaults.columns)
    - filters: словарь с drop_rules и in_rules (если пустые [], используются из defaults)
    """
    # Ключ файла (для идентификации, например "OD_01", "OD_02")
    key: str
    
    # Подпись для логов (например "OD Январь", "OD Февраль")
    label: str
    
    # Имя файла в каталоге IN (например "OD_01.xlsx")
    # Если пустое "", файл не используется
    file_name: str
    
    # Название листа для чтения (если None, используется default_sheet из группы)
    sheet: Optional[str] = None
    
    # Колонки для этого файла (если пустой массив [], используются из defaults.columns)
    # Формат: [{"alias": "tb", "source": "Короткое ТБ"}, ...]
    columns: List[Dict[str, str]] = field(default_factory=list)
    
    # Фильтры для этого файла
    # Формат: {"drop_rules": [...], "in_rules": [...]}
    # drop_rules: список словарей {"alias": "...", "values": [...], "remove_unconditionally": True, ...}
    # in_rules: список словарей {"alias": "...", "values": [...], "condition": "in" или "not_in"}
    # Если drop_rules или in_rules пустые массивы [], используются из defaults
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GroupConfig:
    """Конфигурация для группы файлов (OD, RA, PS)."""
    # Название группы
    name: str
    
    # Лист по умолчанию (если у конкретного файла другой лист, задайте его в items)
    default_sheet: str = "Sheet1"
    
    # Список файлов (items) - для каждого файла указываем key, label, file_name и параметры
    # Если file_name пустое "", файл не используется
    # Если columns или filters.drop_rules пустые массивы [], используются значения из defaults
    items: List[FileItem] = field(default_factory=list)
    
    # Колонки по умолчанию для этой группы
    # Формат: [{"alias": "tb", "source": "Короткое ТБ"}, ...]
    # alias - внутреннее имя колонки (английское)
    # source - имя колонки в Excel файле (русское)
    default_columns: List[Dict[str, str]] = field(default_factory=list)
    
    # Правила удаления строк по умолчанию (drop_rules)
    # Если в items для файла drop_rules пустой массив [], используются эти правила
    default_drop_rules: List[DropRule] = field(default_factory=list)
    
    # Правила включения строк по умолчанию (in_rules)
    # Если в items для файла in_rules пустой массив [], используются эти правила
    default_in_rules: List[IncludeRule] = field(default_factory=list)
    
    # Название колонки с табельным номером (используется alias после маппинга)
    tab_number_column: str = "tab_number"
    
    # Название колонки с ТБ (используется alias после маппинга)
    tb_column: str = "tb"
    
    # Название колонки с ГОСБ (используется alias после маппинга)
    gosb_column: str = "gosb"
    
    # Название колонки с ФИО (используется alias после маппинга)
    fio_column: str = "fio"
    
    # Название колонки с показателем (используется alias после маппинга)
    indicator_column: str = "indicator"
    
    # Дополнительные параметры обработки файлов
    # Номер строки с заголовками (0 - первая строка, None - автоматическое определение)
    header_row: Optional[int] = 0
    
    # Количество строк для пропуска в начале файла
    skip_rows: int = 0
    
    # Количество строк для пропуска в конце файла
    skip_footer: int = 0
    
    # Название листа для чтения (None - первый лист)
    sheet_name: Optional[str] = None
    
    # Номер листа для чтения (0 - первый лист, None - использовать sheet_name)
    sheet_index: Optional[int] = None


class ConfigManager:
    """Менеджер конфигурации для управления настройками загрузки файлов."""
    
    def __init__(self):
        """Инициализация менеджера конфигурации с настройками по умолчанию."""
        self.groups: Dict[str, GroupConfig] = self._create_default_configs()
    
    def _create_default_configs(self) -> Dict[str, GroupConfig]:
        """
        Создает конфигурации по умолчанию для всех групп.
        
        Returns:
            Dict[str, GroupConfig]: Словарь с конфигурациями групп
        """
        configs = {}
        
        # Конфигурация для группы OD (ОперДоход)
        configs["OD"] = GroupConfig(
            name="OD",
            default_sheet="Sheet1",
            items=[
                FileItem(key="OD_01", label="OD Январь", file_name="M-1_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_02", label="OD Февраль", file_name="M-2_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_03", label="OD Март", file_name="M-3_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_04", label="OD Апрель", file_name="M-4_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_05", label="OD Май", file_name="M-5_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_06", label="OD Июнь", file_name="M-6_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_07", label="OD Июль", file_name="M-7_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_08", label="OD Август", file_name="M-8_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_09", label="OD Сентябрь", file_name="M-9_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_10", label="OD Октябрь", file_name="M-10_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_11", label="OD Ноябрь", file_name="M-11_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="OD_12", label="OD Декабрь", file_name="M-12_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
            ],
            # Колонки по умолчанию: маппинг source (имя в Excel) -> alias (внутреннее имя)
            default_columns=[
                {"alias": "tab_number", "source": "Табельный номер"},
                {"alias": "tb", "source": "Короткое ТБ"},
                {"alias": "gosb", "source": "Полное ГОСБ"},
                {"alias": "client_id", "source": "ИНН"},
                {"alias": "fio", "source": "ФИО"},
                {"alias": "indicator", "source": "Факт"}
            ],
            # Правила удаления строк по умолчанию (drop_rules)
            # Если в items для файла drop_rules пустой массив [], используются эти правила
            # 
            # Параметры DropRule:
            #   - alias: имя поля после маппинга (из default_columns)
            #   - values: список запрещенных значений
            #   - remove_unconditionally: True - удалять всегда, False - не удалять
            #   - check_by_inn: True - не удалять, если по ИНН есть другие значения
            #   - check_by_tn: True - не удалять, если по ТН есть другие значения
            #
            # Примеры:
            #   DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False)
            #   DropRule(alias="tb", values=["ЦА"], remove_unconditionally=True, check_by_inn=True, check_by_tn=False)
            default_drop_rules=[
                # DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False),
            ],
            # Правила включения строк по умолчанию (in_rules)
            # Если в items для файла in_rules пустой массив [], используются эти правила
            # Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (И)
            #
            # Параметры IncludeRule:
            #   - alias: имя поля после маппинга (из default_columns)
            #   - values: список разрешенных значений
            #   - condition: "in" - значение должно быть в списке, "not_in" - не должно быть
            #
            # Примеры:
            #   IncludeRule(alias="type", values=["Активен"], condition="in")
            #   IncludeRule(alias="tb", values=["ЦА"], condition="not_in")
            default_in_rules=[
                # IncludeRule(alias="type", values=["Активен"], condition="in"),
            ],
            # Имена колонок после маппинга (используются alias)
            tab_number_column="tab_number",
            tb_column="tb",
            gosb_column="gosb",
            fio_column="fio",
            indicator_column="indicator",
            header_row=0,
            skip_rows=0,
            skip_footer=0,
            sheet_name=None,
            sheet_index=None
        )
        
        # Конфигурация для группы RA (Работающие Активы/кредиты)
        configs["RA"] = GroupConfig(
            name="RA",
            default_sheet="Sheet1",
            items=[
                FileItem(key="RA_01", label="RA Январь", file_name="M-1_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_02", label="RA Февраль", file_name="M-2_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_03", label="RA Март", file_name="M-3_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_04", label="RA Апрель", file_name="M-4_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_05", label="RA Май", file_name="M-5_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_06", label="RA Июнь", file_name="M-6_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_07", label="RA Июль", file_name="M-7_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_08", label="RA Август", file_name="M-8_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_09", label="RA Сентябрь", file_name="M-9_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_10", label="RA Октябрь", file_name="M-10_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_11", label="RA Ноябрь", file_name="M-11_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="RA_12", label="RA Декабрь", file_name="M-12_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
            ],
            # Колонки по умолчанию: маппинг source (имя в Excel) -> alias (внутреннее имя)
            default_columns=[
                {"alias": "tab_number", "source": "Табельный номер"},
                {"alias": "tb", "source": "Короткое ТБ"},
                {"alias": "gosb", "source": "Полное ГОСБ"},
                {"alias": "client_id", "source": "ИНН"},
                {"alias": "fio", "source": "ФИО"},
                {"alias": "indicator", "source": "Факт"}
            ],
            # Правила удаления строк по умолчанию (drop_rules)
            default_drop_rules=[
                # DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False),
            ],
            # Правила включения строк по умолчанию (in_rules)
            default_in_rules=[
                # IncludeRule(alias="type", values=["Активен"], condition="in"),
            ],
            # Имена колонок после маппинга (используются alias)
            tab_number_column="tab_number",
            tb_column="tb",
            gosb_column="gosb",
            fio_column="fio",
            indicator_column="indicator",
            header_row=0,
            skip_rows=0,
            skip_footer=0,
            sheet_name=None,
            sheet_index=None
        )
        
        # Конфигурация для группы PS (Пассивы)
        configs["PS"] = GroupConfig(
            name="PS",
            default_sheet="Sheet1",
            items=[
                FileItem(key="PS_01", label="PS Январь", file_name="M-1_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_02", label="PS Февраль", file_name="M-2_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_03", label="PS Март", file_name="M-3_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_04", label="PS Апрель", file_name="M-4_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_05", label="PS Май", file_name="M-5_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_06", label="PS Июнь", file_name="M-6_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_07", label="PS Июль", file_name="M-7_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_08", label="PS Август", file_name="M-8_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_09", label="PS Сентябрь", file_name="M-9_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_10", label="PS Октябрь", file_name="M-10_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_11", label="PS Ноябрь", file_name="M-11_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
                FileItem(key="PS_12", label="PS Декабрь", file_name="M-12_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}),
            ],
            # Колонки по умолчанию: маппинг source (имя в Excel) -> alias (внутреннее имя)
            default_columns=[
                {"alias": "tab_number", "source": "Табельный номер"},
                {"alias": "tb", "source": "Короткое ТБ"},
                {"alias": "gosb", "source": "Полное ГОСБ"},
                {"alias": "client_id", "source": "ИНН"},
                {"alias": "fio", "source": "ФИО"},
                {"alias": "indicator", "source": "Факт"}
            ],
            # Правила удаления строк по умолчанию (drop_rules)
            default_drop_rules=[
                # DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False),
            ],
            # Правила включения строк по умолчанию (in_rules)
            default_in_rules=[
                # IncludeRule(alias="type", values=["Активен"], condition="in"),
            ],
            # Имена колонок после маппинга (используются alias)
            tab_number_column="tab_number",
            tb_column="tb",
            gosb_column="gosb",
            fio_column="fio",
            indicator_column="indicator",
            header_row=0,
            skip_rows=0,
            skip_footer=0,
            sheet_name=None,
            sheet_index=None
        )
        
        return configs
    
    def get_file_item(self, group_name: str, file_name: str) -> Optional[FileItem]:
        """
        Получает конфигурацию элемента файла (FileItem) по имени файла.
        
        Args:
            group_name: Название группы (OD, RA, PS)
            file_name: Имя файла
            
        Returns:
            Optional[FileItem]: Элемент конфигурации файла или None
        """
        if group_name not in self.groups:
            return None
        
        group_config = self.groups[group_name]
        
        # Ищем файл в items по file_name
        for item in group_config.items:
            if item.file_name == file_name:
                return item
        
        return None
    
    def get_config_for_file(self, group_name: str, file_name: str) -> Dict[str, Any]:
        """
        Получает конфигурацию для конкретного файла.
        
        Args:
            group_name: Название группы (OD, RA, PS)
            file_name: Имя файла
            
        Returns:
            Dict[str, Any]: Конфигурация для файла
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        group_config = self.groups[group_name]
        
        # Ищем элемент файла в items
        file_item = self.get_file_item(group_name, file_name)
        
        # Формируем итоговую конфигурацию
        # Колонки: если в item есть columns и он не пустой, используем их, иначе defaults
        if file_item and file_item.columns:
            columns = file_item.columns
        else:
            columns = group_config.default_columns
        
        # Правила удаления: если в item есть filters.drop_rules и он не пустой, используем их, иначе defaults
        if file_item and file_item.filters.get("drop_rules"):
            # Преобразуем словари в DropRule объекты
            drop_rules = [
                DropRule(
                    alias=rule["alias"],
                    values=rule["values"],
                    remove_unconditionally=rule.get("remove_unconditionally", True),
                    check_by_inn=rule.get("check_by_inn", False),
                    check_by_tn=rule.get("check_by_tn", False)
                ) for rule in file_item.filters["drop_rules"]
            ]
        else:
            drop_rules = group_config.default_drop_rules
        
        # Правила включения: если в item есть filters.in_rules и он не пустой, используем их, иначе defaults
        if file_item and file_item.filters.get("in_rules"):
            # Преобразуем словари в IncludeRule объекты
            in_rules = [
                IncludeRule(
                    alias=rule["alias"],
                    values=rule["values"],
                    condition=rule.get("condition", "in")
                ) for rule in file_item.filters["in_rules"]
            ]
        else:
            in_rules = group_config.default_in_rules
        
        # Лист: если в item есть sheet, используем его, иначе default_sheet группы
        sheet_name = file_item.sheet if file_item and file_item.sheet else group_config.default_sheet
        
        result = {
            "columns": columns,
            "drop_rules": drop_rules,
            "in_rules": in_rules,
            "tab_number_column": group_config.tab_number_column,
            "tb_column": group_config.tb_column,
            "gosb_column": group_config.gosb_column,
            "fio_column": group_config.fio_column,
            "indicator_column": group_config.indicator_column,
            "header_row": group_config.header_row,
            "skip_rows": group_config.skip_rows,
            "skip_footer": group_config.skip_footer,
            "sheet_name": sheet_name,
            "sheet_index": group_config.sheet_index,
            "label": file_item.label if file_item else file_name
        }
        
        return result
    
    def add_file_item(self, group_name: str, file_item: FileItem) -> None:
        """
        Добавляет элемент файла в конфигурацию группы.
        
        Args:
            group_name: Название группы
            file_item: Элемент конфигурации файла
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        self.groups[group_name].items.append(file_item)
    
    def get_group_config(self, group_name: str) -> GroupConfig:
        """
        Получает конфигурацию группы.
        
        Args:
            group_name: Название группы
            
        Returns:
            GroupConfig: Конфигурация группы
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        return self.groups[group_name]


# Глобальный экземпляр менеджера конфигурации
config_manager = ConfigManager()


# ============================================================================
# МОДУЛЬ ЛОГИРОВАНИЯ
# ============================================================================

class Logger:
    """Класс для настройки и управления логированием."""
    
    def __init__(self, log_dir: str = LOG_DIR, level: str = LOG_LEVEL, theme: str = LOG_THEME):
        """
        Инициализация логгера.
        
        Args:
            log_dir: Директория для хранения логов
            level: Уровень логирования (INFO или DEBUG)
            theme: Тема логов (используется в имени файла)
        """
        self.log_dir = Path(log_dir)
        self.level = level.upper()
        self.theme = theme
        
        # Создаем директорию для логов, если её нет
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Настраиваем логгер
        self.logger = logging.getLogger("YEAR_SPOD_TOP_Month")
        self.logger.setLevel(getattr(logging, self.level, logging.INFO))
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Создаем файловый обработчик
        log_file = self._generate_log_filename()
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Создаем форматтер для DEBUG уровня
        debug_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s [class: %(name)s | def: %(funcName)s]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(debug_formatter)
        
        # Создаем консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Добавляем обработчики
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _generate_log_filename(self) -> Path:
        """
        Генерирует имя файла лога по шаблону.
        
        Returns:
            Path: Путь к файлу лога
        """
        now = datetime.now()
        filename = f"{self.level}_{self.theme}_{now.strftime('%Y%m%d_%H')}.log"
        return self.log_dir / filename
    
    def info(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня INFO.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.info(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.info(message)
    
    def debug(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня DEBUG.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.debug(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.debug(message)
    
    def warning(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня WARNING.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.warning(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.warning(message)
    
    def error(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня ERROR.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.error(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.error(message)


# ============================================================================
# МОДУЛЬ ОБРАБОТКИ ФАЙЛОВ
# ============================================================================

class FileProcessor:
    """Класс для обработки Excel файлов."""
    
    def __init__(self, input_dir: str = INPUT_DIR, logger_instance: Optional[Logger] = None):
        """
        Инициализация процессора файлов.
        
        Args:
            input_dir: Путь к каталогу с входными данными
            logger_instance: Экземпляр логгера
        """
        self.input_dir = Path(input_dir)
        self.groups = ["OD", "RA", "PS"]
        self.processed_files: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.unique_tab_numbers: Dict[str, Dict[str, Any]] = {}
        self.logger = logger_instance
    
    def load_all_files(self) -> None:
        """
        Загружает все файлы из подкаталогов OD, RA, PS.
        
        Файлы загружаются с учетом конфигурации для каждой группы.
        Используются только файлы из списка expected_files.
        """
        self.logger.info("Начало загрузки файлов", "FileProcessor", "load_all_files")
        
        for group in self.groups:
            group_path = self.input_dir / group
            if not group_path.exists():
                self.logger.warning(f"Каталог {group_path} не найден, пропускаем", "FileProcessor", "load_all_files")
                continue
            
            self.logger.info(f"Обработка группы {group}", "FileProcessor", "load_all_files")
            self.processed_files[group] = {}
            
            # Получаем конфигурацию группы
            group_config = config_manager.get_group_config(group)
            items = group_config.items
            
            if not items:
                self.logger.warning(f"Список файлов (items) пуст для группы {group}", "FileProcessor", "load_all_files")
                continue
            
            self.logger.debug(f"Ожидается {len(items)} файлов в группе {group}", "FileProcessor", "load_all_files")
            
            # Загружаем только файлы из списка items
            for item in items:
                # Пропускаем файлы с пустым file_name
                if not item.file_name or item.file_name.strip() == "":
                    self.logger.debug(f"Файл с ключом {item.key} имеет пустое file_name, пропускаем", "FileProcessor", "load_all_files")
                    continue
                
                file_path = group_path / item.file_name
                
                if not file_path.exists():
                    self.logger.debug(f"Файл {item.file_name} (ключ: {item.key}, метка: {item.label}) не найден, пропускаем", "FileProcessor", "load_all_files")
                    continue
                
                try:
                    df = self._load_file(file_path, group)
                    if df is not None and not df.empty:
                        self.processed_files[group][file_path.name] = df
                        self.logger.debug(f"Загружен файл {item.file_name} ({item.label}, {len(df)} строк)", "FileProcessor", "load_all_files")
                    else:
                        self.logger.warning(f"Файл {item.file_name} ({item.label}) загружен, но пуст", "FileProcessor", "load_all_files")
                except Exception as e:
                    self.logger.error(f"Ошибка при загрузке файла {item.file_name} ({item.label}): {str(e)}", "FileProcessor", "load_all_files")
        
        self.logger.info(f"Загрузка завершена. Обработано групп: {len(self.processed_files)}", "FileProcessor", "load_all_files")
    
    def _load_file(self, file_path: Path, group_name: str) -> Optional[pd.DataFrame]:
        """
        Загружает один файл с применением конфигурации.
        
        Args:
            file_path: Путь к файлу
            group_name: Название группы
            
        Returns:
            Optional[pd.DataFrame]: DataFrame с данными или None при ошибке
        """
        try:
            # Получаем конфигурацию для файла
            config = config_manager.get_config_for_file(group_name, file_path.name)
            
            self.logger.debug(f"Загрузка файла {file_path.name} с конфигурацией: {config}", "FileProcessor", "_load_file")
            
            # Подготавливаем параметры для чтения Excel
            read_params = {}
            
            # Определяем engine
            if OPENPYXL_AVAILABLE:
                read_params['engine'] = 'openpyxl'
            
            # Параметры листа
            # Используем sheet_name из конфигурации (может быть из item.sheet или default_sheet)
            if config["sheet_name"]:
                read_params['sheet_name'] = config["sheet_name"]
            elif config["sheet_index"] is not None:
                read_params['sheet_name'] = config["sheet_index"]
            
            # Параметры пропуска строк
            if config["skip_rows"] > 0:
                read_params['skiprows'] = config["skip_rows"]
            
            if config["skip_footer"] > 0:
                read_params['skipfooter'] = config["skip_footer"]
            
            # Параметр заголовка
            if config["header_row"] is not None:
                read_params['header'] = config["header_row"]
            
            # Загружаем Excel файл
            try:
                df = pd.read_excel(file_path, **read_params)
            except Exception as e:
                # Если не удалось загрузить с параметрами, пробуем без них
                self.logger.warning(f"Ошибка при загрузке с параметрами, пробуем без них: {str(e)}", "FileProcessor", "_load_file")
                try:
                    df = pd.read_excel(file_path)
                except Exception as e2:
                    self.logger.error(f"Не удалось загрузить файл {file_path.name}: {str(e2)}", "FileProcessor", "_load_file")
                    return None
            
            # Нормализуем названия колонок (убираем пробелы)
            df.columns = df.columns.str.strip()
            
            # Применяем маппинг колонок (source -> alias)
            if config["columns"]:
                # Формируем словарь маппинга: source -> alias
                column_maps = {col["source"]: col["alias"] for col in config["columns"]}
                
                # Проверяем наличие всех source колонок
                missing_columns = [col["source"] for col in config["columns"] if col["source"] not in df.columns]
                if missing_columns:
                    self.logger.warning(f"Отсутствующие колонки в файле {file_path.name}: {missing_columns}", "FileProcessor", "_load_file")
                
                # Переименовываем колонки из source в alias
                available_maps = {k: v for k, v in column_maps.items() if k in df.columns}
                df = df.rename(columns=available_maps)
                
                # Оставляем только нужные колонки (по alias)
                required_columns = [col["alias"] for col in config["columns"]]
                available_columns = [col for col in required_columns if col in df.columns]
                df = df[available_columns]
            
            # Применяем правила удаления строк (drop_rules)
            if config["drop_rules"]:
                df = self._apply_drop_rules(df, config["drop_rules"], file_path.name)
            
            # Применяем правила включения строк (in_rules)
            if config["in_rules"]:
                df = self._apply_in_rules(df, config["in_rules"], file_path.name)
            
            # Добавляем метаданные о файле
            df.attrs['file_name'] = file_path.name
            df.attrs['group_name'] = group_name
            df.attrs['file_path'] = str(file_path)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}", "FileProcessor", "_load_file")
            return None
    
    def _apply_drop_rules(self, df: pd.DataFrame, drop_rules: List[DropRule], file_name: str) -> pd.DataFrame:
        """
        Применяет правила удаления строк (drop_rules).
        
        Args:
            df: DataFrame для обработки
            drop_rules: Список правил удаления
            file_name: Имя файла для логирования
            
        Returns:
            DataFrame после применения правил
        """
        cleaned = df.copy()
        
        for rule in drop_rules:
            if rule.alias not in cleaned.columns:
                self.logger.warning(f"Колонка {rule.alias} отсутствует в файле {file_name}, пропускаем правило", "FileProcessor", "_apply_drop_rules")
                continue
            
            if not rule.remove_unconditionally:
                self.logger.debug(f"Колонка {rule.alias}: remove_unconditionally=False, строки не удаляются", "FileProcessor", "_apply_drop_rules")
                continue
            
            # Формируем множество запрещенных значений (в нижнем регистре для сравнения)
            forbidden = {str(v).strip().lower() for v in rule.values}
            
            def is_forbidden(value: Any) -> bool:
                """Проверяет, является ли значение запрещенным."""
                if pd.isna(value):
                    return False
                return str(value).strip().lower() in forbidden
            
            # Находим строки с запрещенными значениями
            mask_forbidden = cleaned[rule.alias].apply(is_forbidden)
            
            if not mask_forbidden.any():
                self.logger.debug(f"Колонка {rule.alias}: запрещенных значений не найдено", "FileProcessor", "_apply_drop_rules")
                continue
            
            if not rule.check_by_inn and not rule.check_by_tn:
                # Простое удаление без условий
                before = len(cleaned)
                cleaned = cleaned[~mask_forbidden]
                self.logger.debug(f"Колонка {rule.alias}: удалено {before - len(cleaned)} строк (безусловно)", "FileProcessor", "_apply_drop_rules")
            else:
                # Условное удаление
                rows_to_remove = mask_forbidden.copy()
                
                for idx in cleaned[mask_forbidden].index:
                    row = cleaned.loc[idx]
                    should_keep = False
                    
                    # Проверка по ИНН (если есть колонка client_id)
                    if rule.check_by_inn and "client_id" in cleaned.columns:
                        client_id = row.get("client_id")
                        if pd.notna(client_id):
                            other_rows = cleaned[(cleaned["client_id"] == client_id) & (cleaned.index != idx)]
                            if len(other_rows) > 0:
                                other_values = other_rows[rule.alias].apply(lambda v: not is_forbidden(v) if pd.notna(v) else False)
                                if other_values.any():
                                    should_keep = True
                    
                    # Проверка по ТН (если есть колонка tab_number или manager_id)
                    if rule.check_by_tn:
                        tab_col = None
                        if "tab_number" in cleaned.columns:
                            tab_col = "tab_number"
                        elif "manager_id" in cleaned.columns:
                            tab_col = "manager_id"
                        
                        if tab_col:
                            tab_id = row.get(tab_col)
                            if pd.notna(tab_id):
                                other_rows = cleaned[(cleaned[tab_col] == tab_id) & (cleaned.index != idx)]
                                if len(other_rows) > 0:
                                    other_values = other_rows[rule.alias].apply(lambda v: not is_forbidden(v) if pd.notna(v) else False)
                                    if other_values.any():
                                        should_keep = True
                    
                    if should_keep:
                        rows_to_remove.loc[idx] = False
                
                before = len(cleaned)
                cleaned = cleaned[~rows_to_remove]
                self.logger.debug(
                    f"Колонка {rule.alias}: удалено {before - len(cleaned)} строк "
                    f"(условно: remove_unconditionally={rule.remove_unconditionally}, "
                    f"check_by_inn={rule.check_by_inn}, check_by_tn={rule.check_by_tn})",
                    "FileProcessor", "_apply_drop_rules"
                )
        
        return cleaned
    
    def _apply_in_rules(self, df: pd.DataFrame, in_rules: List[IncludeRule], file_name: str) -> pd.DataFrame:
        """
        Применяет правила включения строк (in_rules).
        
        Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (И).
        
        Args:
            df: DataFrame для обработки
            in_rules: Список правил включения
            file_name: Имя файла для логирования
            
        Returns:
            DataFrame после применения правил
        """
        if not in_rules:
            return df
        
        # Начинаем с маски True для всех строк
        final_mask = pd.Series(True, index=df.index)
        
        for rule in in_rules:
            if rule.alias not in df.columns:
                self.logger.warning(f"Колонка {rule.alias} отсутствует в файле {file_name}, пропускаем правило", "FileProcessor", "_apply_in_rules")
                continue
            
            # Формируем множество разрешенных значений
            allowed = {str(v).strip().lower() for v in rule.values}
            
            def check_value(value: Any) -> bool:
                """Проверяет значение по условию."""
                if pd.isna(value):
                    return False
                value_str = str(value).strip().lower()
                if rule.condition == "in":
                    return value_str in allowed
                elif rule.condition == "not_in":
                    return value_str not in allowed
                return False
            
            # Применяем условие (И - все условия должны выполняться)
            rule_mask = df[rule.alias].apply(check_value)
            final_mask = final_mask & rule_mask
        
        before = len(df)
        result = df[final_mask]
        self.logger.debug(f"После применения in_rules: оставлено {len(result)} строк из {before}", "FileProcessor", "_apply_in_rules")
        
        return result
    
    def collect_unique_tab_numbers(self) -> None:
        """
        Собирает уникальные табельные номера из всех файлов.
        
        Приоритет данных:
        1. Группы: OD > RA > PS
        2. Месяцы: 12 > 11 > ... > 1
        
        Для каждого табельного номера сохраняется ТБ, ГОСБ, ФИО из файла с наивысшим приоритетом.
        """
        self.logger.info("Начало сбора уникальных табельных номеров", "FileProcessor", "collect_unique_tab_numbers")
        
        # Порядок приоритета групп
        group_priority = {"OD": 1, "RA": 2, "PS": 3}
        
        # Извлекаем номер месяца из имени файла
        def extract_month_number(file_name: str) -> int:
            """
            Извлекает номер месяца из имени файла.
            
            Поддерживает форматы:
            - M-{номер}_{группа}.xlsx (например, M-1_RA.xlsx, M-12_OD.xlsx)
            - {группа}_{номер}.xlsx (например, RA_01.xlsx, OD_12.xlsx)
            - T-{номер} (например, T-11, T-0) - где T-11 = январь, T-0 = декабрь
            
            Args:
                file_name: Имя файла
                
            Returns:
                int: Номер месяца (1-12) или 0, если не удалось определить
            """
            # Паттерн для формата M-{номер}_{группа}.xlsx
            match = re.search(r'M-(\d{1,2})_', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    return month
            
            # Паттерн для формата {группа}_{номер}.xlsx (например, RA_01.xlsx)
            match = re.search(r'_(\d{2})\.', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    return month
            
            # Паттерн для формата T-{номер} (T-11 = январь, T-0 = декабрь)
            match = re.search(r'T-(\d{1,2})', file_name)
            if match:
                t_value = int(match.group(1))
                # Преобразуем T-11 -> 1 (январь), T-0 -> 12 (декабрь)
                if 0 <= t_value <= 11:
                    month = 12 - t_value
                    if 1 <= month <= 12:
                        return month
            
            # Если не нашли, возвращаем 0 (низкий приоритет)
            return 0
        
        # Собираем все табельные номера с информацией о файлах
        all_tab_data: Dict[str, Dict[str, Any]] = {}
        
        # Проходим по группам в порядке приоритета
        for group in sorted(self.groups, key=lambda x: group_priority.get(x, 999)):
            if group not in self.processed_files:
                continue
            
            group_config = config_manager.get_group_config(group)
            tab_col = group_config.tab_number_column
            tb_col = group_config.tb_column
            gosb_col = group_config.gosb_column
            fio_col = group_config.fio_column
            
            # Сортируем файлы по номеру месяца (от большего к меньшему)
            files_sorted = sorted(
                self.processed_files[group].items(),
                key=lambda x: extract_month_number(x[0]),
                reverse=True
            )
            
            for file_name, df in files_sorted:
                month = extract_month_number(file_name)
                self.logger.debug(f"Обработка файла {file_name} группы {group}, месяц {month}", "FileProcessor", "collect_unique_tab_numbers")
                
                if tab_col not in df.columns:
                    self.logger.warning(f"Колонка '{tab_col}' не найдена в файле {file_name}", "FileProcessor", "collect_unique_tab_numbers")
                    continue
                
                # Обрабатываем каждую строку
                for idx, row in df.iterrows():
                    tab_number = str(row[tab_col]).strip() if pd.notna(row[tab_col]) else None
                    
                    if not tab_number or tab_number == 'nan':
                        continue
                    
                    # Если табельный номер еще не встречался, или текущий файл имеет более высокий приоритет
                    if tab_number not in all_tab_data:
                        all_tab_data[tab_number] = {
                            "tab_number": tab_number,
                            "tb": row[tb_col] if tb_col in df.columns and pd.notna(row[tb_col]) else "",
                            "gosb": row[gosb_col] if gosb_col in df.columns and pd.notna(row[gosb_col]) else "",
                            "fio": row[fio_col] if fio_col in df.columns and pd.notna(row[fio_col]) else "",
                            "group": group,
                            "month": month,
                            "priority": group_priority[group] * 100 + month  # Комбинированный приоритет
                        }
                    else:
                        # Проверяем, нужно ли обновить данные
                        current_priority = group_priority[group] * 100 + month
                        if current_priority < all_tab_data[tab_number]["priority"]:
                            all_tab_data[tab_number].update({
                                "tb": row[tb_col] if tb_col in df.columns and pd.notna(row[tb_col]) else all_tab_data[tab_number]["tb"],
                                "gosb": row[gosb_col] if gosb_col in df.columns and pd.notna(row[gosb_col]) else all_tab_data[tab_number]["gosb"],
                                "fio": row[fio_col] if fio_col in df.columns and pd.notna(row[fio_col]) else all_tab_data[tab_number]["fio"],
                                "group": group,
                                "month": month,
                                "priority": current_priority
                            })
        
        self.unique_tab_numbers = all_tab_data
        self.logger.info(f"Собрано {len(self.unique_tab_numbers)} уникальных табельных номеров", "FileProcessor", "collect_unique_tab_numbers")
    
    def prepare_summary_data(self) -> pd.DataFrame:
        """
        Подготавливает сводные данные для итогового файла.
        
        Для каждого табельного номера собирает суммы показателей из каждого файла.
        
        Returns:
            pd.DataFrame: DataFrame со сводными данными
        """
        self.logger.info("Начало подготовки сводных данных", "FileProcessor", "prepare_summary_data")
        
        if not self.unique_tab_numbers:
            self.logger.warning("Уникальные табельные номера не собраны", "FileProcessor", "prepare_summary_data")
            self.collect_unique_tab_numbers()
        
        # Создаем список всех файлов в порядке обработки
        all_files: List[Tuple[str, str, str]] = []  # (group, file_name, full_name)
        
        for group in self.groups:
            if group in self.processed_files:
                for file_name in sorted(self.processed_files[group].keys()):
                    full_name = f"{group}_{file_name}"
                    all_files.append((group, file_name, full_name))
        
        # Создаем структуру данных
        result_data = []
        
        for tab_number, tab_info in self.unique_tab_numbers.items():
            row = {
                "Табельный": tab_number,
                "ТБ": tab_info["tb"],
                "ГОСБ": tab_info["gosb"],
                "ФИО": tab_info["fio"]
            }
            
            # Для каждого файла собираем сумму показателя
            for group, file_name, full_name in all_files:
                if group in self.processed_files and file_name in self.processed_files[group]:
                    df = self.processed_files[group][file_name]
                    group_config = config_manager.get_group_config(group)
                    tab_col = group_config.tab_number_column
                    indicator_col = group_config.indicator_column
                    
                    # Фильтруем данные по табельному номеру
                    tab_data = df[df[tab_col].astype(str).str.strip() == str(tab_number)]
                    
                    if not tab_data.empty and indicator_col in tab_data.columns:
                        # Суммируем показатель
                        total = tab_data[indicator_col].sum()
                        row[full_name] = total
                    else:
                        row[full_name] = 0
            
            result_data.append(row)
        
        result_df = pd.DataFrame(result_data)
        self.logger.info(f"Подготовлено {len(result_df)} строк сводных данных", "FileProcessor", "prepare_summary_data")
        
        return result_df


# ============================================================================
# МОДУЛЬ ФОРМАТИРОВАНИЯ EXCEL
# ============================================================================

class ExcelFormatter:
    """Класс для форматирования Excel файлов с использованием только базовых модулей Anaconda."""
    
    def __init__(self, logger_instance: Optional[Logger] = None):
        """
        Инициализация форматтера.
        
        Args:
            logger_instance: Экземпляр логгера
        """
        self.min_width = 15
        self.max_width = 150
        self.logger = logger_instance
    
    def _calculate_column_width(self, df: pd.DataFrame, col_name: str) -> float:
        """
        Вычисляет оптимальную ширину колонки на основе содержимого.
        
        Args:
            df: DataFrame с данными
            col_name: Название колонки
            
        Returns:
            float: Ширина колонки
        """
        if col_name not in df.columns:
            return self.min_width
        
        # Максимальная длина в заголовке
        max_length = len(str(col_name))
        
        # Максимальная длина в данных (первые 100 строк для производительности)
        sample_size = min(100, len(df))
        for idx in range(sample_size):
            value = df[col_name].iloc[idx]
            if pd.notna(value):
                max_length = max(max_length, len(str(value)))
        
        # Применяем ограничения
        width = max(self.min_width, min(max_length + 2, self.max_width))
        return width
    
    def create_formatted_excel(self, df: pd.DataFrame, output_path: str, sheet_name: str = "Данные") -> None:
        """
        Создает новый Excel файл с форматированием используя только базовые модули Anaconda.
        Приоритет: openpyxl > xlsxwriter > без форматирования
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения файла
            sheet_name: Название листа
        """
        self.logger.info(f"Создание форматированного Excel файла {output_path}", "ExcelFormatter", "create_formatted_excel")
        
        try:
            if OPENPYXL_AVAILABLE:
                # Используем openpyxl для форматирования (приоритетный вариант)
                self._create_with_openpyxl(df, output_path, sheet_name)
            elif XLSXWRITER_AVAILABLE:
                # Используем xlsxwriter для форматирования (если openpyxl недоступен)
                self._create_with_xlsxwriter(df, output_path, sheet_name)
            else:
                # Используем pandas ExcelWriter без форматирования
                self.logger.warning("openpyxl и xlsxwriter недоступны, создается файл без форматирования", "ExcelFormatter", "create_formatted_excel")
                # Пробуем использовать доступный engine
                try:
                    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                except:
                    try:
                        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except:
                        # Если не получилось, используем любой доступный engine
                        with pd.ExcelWriter(output_path) as writer:
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                self.logger.info(f"Файл {output_path} создан без форматирования", "ExcelFormatter", "create_formatted_excel")
            
        except Exception as e:
            self.logger.error(f"Ошибка при создании Excel файла {output_path}: {str(e)}", "ExcelFormatter", "create_formatted_excel")
            # Пробуем создать без форматирования
            try:
                with pd.ExcelWriter(output_path) as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                self.logger.warning(f"Файл создан без форматирования из-за ошибки: {str(e)}", "ExcelFormatter", "create_formatted_excel")
            except Exception as e2:
                self.logger.error(f"Критическая ошибка при создании файла: {str(e2)}", "ExcelFormatter", "create_formatted_excel")
                raise
    
    def _create_with_openpyxl(self, df: pd.DataFrame, output_path: str, sheet_name: str = "Данные") -> None:
        """
        Создает Excel файл с форматированием используя openpyxl.
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения файла
            sheet_name: Название листа
        """
        self.logger.info(f"Использование openpyxl для форматирования", "ExcelFormatter", "_create_with_openpyxl")
        
        # Сначала сохраняем DataFrame в Excel через pandas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Теперь форматируем файл
        wb = load_workbook(output_path)
        
        if sheet_name not in wb.sheetnames:
            self.logger.warning(f"Лист {sheet_name} не найден, используем первый лист", "ExcelFormatter", "_create_with_openpyxl")
            ws = wb.active
        else:
            ws = wb[sheet_name]
        
        # Фиксируем первую строку
        ws.freeze_panes = "A2"
        self.logger.debug("Первая строка зафиксирована", "ExcelFormatter", "_create_with_openpyxl")
        
        # Форматируем заголовки (первая строка)
        header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
        header_font = Font(bold=True, size=12)
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        
        for cell in ws[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        self.logger.debug("Заголовки отформатированы", "ExcelFormatter", "_create_with_openpyxl")
        
        # Настраиваем ширину колонок
        for col_idx, column in enumerate(ws.iter_cols(min_row=1, max_row=1), start=1):
            col_letter = get_column_letter(col_idx)
            
            # Вычисляем оптимальную ширину на основе содержимого
            max_length = 0
            for cell in column:
                if cell.value:
                    cell_value = str(cell.value)
                    max_length = max(max_length, len(cell_value))
            
            # Учитываем содержимое всех ячеек в колонке (первые 100 строк для производительности)
            for row in ws.iter_rows(min_row=2, max_row=min(102, ws.max_row), min_col=col_idx, max_col=col_idx):
                for cell in row:
                    if cell.value:
                        cell_value = str(cell.value)
                        max_length = max(max_length, len(cell_value))
            
            # Применяем ограничения
            width = max(self.min_width, min(max_length + 2, self.max_width))
            ws.column_dimensions[col_letter].width = width
            
            self.logger.debug(f"Колонка {col_letter} установлена ширина {width}", "ExcelFormatter", "_create_with_openpyxl")
        
        # Настраиваем выравнивание и перенос текста для всех ячеек
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        
        self.logger.debug("Выравнивание и перенос текста настроены", "ExcelFormatter", "_create_with_openpyxl")
        
        # Включаем автофильтр
        ws.auto_filter.ref = ws.dimensions
        self.logger.debug("Автофильтр включен", "ExcelFormatter", "_create_with_openpyxl")
        
        # Сохраняем файл
        wb.save(output_path)
        self.logger.info(f"Файл {output_path} успешно создан с форматированием (openpyxl)", "ExcelFormatter", "_create_with_openpyxl")
    
    def _create_with_xlsxwriter(self, df: pd.DataFrame, output_path: str, sheet_name: str = "Данные") -> None:
        """
        Создает Excel файл с форматированием используя xlsxwriter.
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения файла
            sheet_name: Название листа
        """
        # Создаем рабочую книгу
        workbook = xlsxwriter.Workbook(output_path)
        worksheet = workbook.add_worksheet(sheet_name)
        
        # Формат для заголовков
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
            'bg_color': '#D3D3D3'
        })
        
        # Формат для данных
        data_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True
        })
        
        # Записываем заголовки
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name, header_format)
            # Вычисляем и устанавливаем ширину колонки
            width = self._calculate_column_width(df, col_name)
            worksheet.set_column(col_idx, col_idx, width)
        
        # Записываем данные
        for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    worksheet.write(row_idx, col_idx, value, data_format)
                else:
                    worksheet.write(row_idx, col_idx, '', data_format)
        
        # Фиксируем первую строку
        worksheet.freeze_panes(1, 0)
        
        # Включаем автофильтр
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        # Закрываем рабочую книгу
        workbook.close()
        
        self.logger.info(f"Файл {output_path} успешно создан с форматированием", "ExcelFormatter", "_create_with_xlsxwriter")


# ============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Основная функция приложения."""
    # Инициализируем логгер
    logger = Logger(log_dir=LOG_DIR, level=LOG_LEVEL, theme=LOG_THEME)
    
    logger.info("=" * 80, "main", "main")
    logger.info("Запуск обработки месячных данных", "main", "main")
    logger.info("=" * 80, "main", "main")
    
    try:
        # Создаем выходной каталог, если его нет
        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Входной каталог: {INPUT_DIR}", "main", "main")
        logger.info(f"Выходной каталог: {OUTPUT_DIR}", "main", "main")
        
        # Инициализируем процессор файлов
        processor = FileProcessor(input_dir=INPUT_DIR, logger_instance=logger)
        
        # Загружаем все файлы
        logger.info("Этап 1: Загрузка файлов", "main", "main")
        processor.load_all_files()
        
        # Собираем уникальные табельные номера
        logger.info("Этап 2: Сбор уникальных табельных номеров", "main", "main")
        processor.collect_unique_tab_numbers()
        
        # Подготавливаем сводные данные
        logger.info("Этап 3: Подготовка сводных данных", "main", "main")
        summary_df = processor.prepare_summary_data()
        
        if summary_df.empty:
            logger.error("Сводные данные пусты, обработка завершена", "main", "main")
            return
        
        # Формируем имя выходного файла с датой и временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"Сводные_данные_{timestamp}.xlsx"
        
        # Создаем форматтер
        formatter = ExcelFormatter(logger_instance=logger)
        
        # Сохраняем данные в Excel с форматированием
        logger.info(f"Этап 4: Сохранение результата в {output_file}", "main", "main")
        formatter.create_formatted_excel(summary_df, str(output_file), sheet_name="Данные")
        
        logger.info("=" * 80, "main", "main")
        logger.info(f"Обработка завершена успешно. Результат сохранен в: {output_file}", "main", "main")
        logger.info(f"Обработано табельных номеров: {len(summary_df)}", "main", "main")
        logger.info(f"Колонок в результате: {len(summary_df.columns)}", "main", "main")
        logger.info("=" * 80, "main", "main")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при выполнении: {str(e)}", "main", "main")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}", "main", "main")
        sys.exit(1)


if __name__ == "__main__":
    main()

